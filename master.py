from py.shared import *
from config import *
import os
import glob
import bson
import json
import paste.urlparser
import gevent
import gevent.wsgi
import zmq.green as zmq
from time import time
from geventwebsocket import WebSocketServer, WebSocketError
import logging
log = logging.getLogger('master')
logging.basicConfig(level=logging.INFO)


# http://css.dzone.com/articles/gevent-zeromq-websockets-and
class MasterApplication(Actionable):
    slave_registry = {}
    sink_registry = {}
    next_slave_id = 0
    next_client_id = 0
    client_registry = {}
    clients = Sender(None)
    removed_slaves = set()

    def __init__(self, context):
        log.info('Master initialized')
        self.context = context
        self.slave_in = context.socket(zmq.PULL)
        self.slave_in.bind("tcp://*:{}".format(SLAVE_MASTER_PORT))
        gevent.spawn(self.listen_to_slaves)

        self.slave_out = context.socket(zmq.PUB)
        self.slave_out.bind("tcp://*:{}".format(SLAVE_PUB_PORT))

        self.slaves = Sender(self.broadcast_data_to_slaves)
        self.clients = Sender(self.broadcast_data_to_clients)

    def broadcast_data_to_slaves(self, data):
        self.slave_out.send(bson.dumps(data))

    def broadcast_data_to_clients(self, data):
        for client in self.client_registry.values():
            client.send_data(data)

    def __call__(self, environ, start_response):
        ws = environ['wsgi.websocket']
        self.listen_to_websocket(ws)

    def listen_to_websocket(self, ws):
        client = self.add_client(ws)
        log.info('Websocket {} connected!'.format(client.id))

        while True:
            try:
                msg = ws.receive()
                client.handle_websocket_message(msg)
            except WebSocketError as e:
                self.disconnect_client(client)
                break

    def get_next_client_id(self):
        self.next_client_id += 1
        return self.next_client_id - 1

    def add_client(self, ws):
        id = self.get_next_client_id()
        client = Client(self, ws, id)
        self.client_registry[id] = client
        return client

    def disconnect_client(self, client):
        if client.id in self.client_registry:
            client.websocket.close()
            del self.client_registry[client.id]

    def listen_to_slaves(self):
        log.info('Listening to slaves')
        while True:
            self.handle_slave_message(self.slave_in.recv())

    def handle_slave_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            if 'slave_id' not in data or data['slave_id'] not in self.slave_registry:
                if data['action'] == 'heartbeat':
                    slave = self.new_slave(data)
                else:
                    log.error('Got non-heartbeat message from unknown slave')
                    return
            else:
                slave = self.load_slave(data)
            slave.run_action(data)

    def load_slave(self, data):
        if 'slave_id' not in data:
            log.warn('load_slave without slave_id')
            return
        slave_id = data['slave_id']
        if slave_id not in self.slave_registry:
            log.warn("load_slave couldn't find slave")
            return
        return self.slave_registry[slave_id]

    def new_slave(self, heartbeat_data):
        slave = Slave(self, self.get_next_slave_id())
        # Modify the data, providing a slave_id to run_action if it needs it
        heartbeat_data['slave_id'] = slave.id

        # Kinda a hack, we want to get the sink_host before the action is run,
        # and that requires heartbeat data, so we store it if we have it
        slave.last_beat = heartbeat_data

        self.slaves.set_id({
            'new_slave_id': slave.id,
            'slave_uuid': heartbeat_data['slave_uuid']
        })
        sink = self.pick_sink(slave)
        self.slaves.set_sink({
            'sink_id': sink.id,
            'sink_host': sink.get_sink_host()
        })
        self.slave_registry[slave.id] = slave
        return slave

    def get_next_slave_id(self):
        self.next_slave_id += 1
        return self.next_slave_id

    def pick_sink(self, slave):
        hostname = slave.get_hostname()
        if hostname:
            if hostname not in self.sink_registry:
                return slave
            else:
                return self.sink_registry[hostname]

    def set_sink(self, slave_id, sink_id, client=None):
        if sink_id not in self.slave_registry:
            log.warn('set_sink to unknown sink')
            if client:
                client.send.error({'error': 'Sink({}) not found'.format(sink_id)})
            return

        sink_host = self.slave_registry[sink_id].get_sink_host()
        if not sink_host:
            log.warn('Could sink_host not found')
            if client:
                client.send.error({'error': 'Sink({}) does not have a host yet'.format(sink_id)})
            return

        data = {
            'sink_host': sink_host,
            'sink_id': sink_id
        }

        self.slaves.set_sink(data)

    def register_sink(self, sink):
        self.sink_registry[sink.get_hostname()] = sink

    def remove_slave(self, id):
        if id in self.slave_registry:
            log.info('Killing slave {}'.format(id))
            del self.slave_registry[id]
        self.slaves.quit({'slave_id': id})
        self.clients.slave_disconnected({'slave_id': id})

    def check_slaves(self):
        the_time = int(time())
        # Since we modify the dict (and that messes with iteration) first get a static list of all of them
        slave_ids = self.slave_registry.keys()
        for slave_id in slave_ids:
            slave = self.slave_registry[slave_id]
            if the_time - slave.last_beat.get('generated', 0) > HEARTBEAT_PERIOD * BEATS_TO_KILL:
                self.remove_slave(slave_id)

    def watch_slaves(self):
        while True:
            self.check_slaves()
            gevent.sleep(HEARTBEAT_PERIOD)

@action_class
class Slave(Actionable):
    sink = None

    def __init__(self, master, id):
        self.master = master
        self.last_beat = {}
        self.id = id
        self.broadcast_to = Sender(self.annotate_broadcast_with_id)

    def annotate_broadcast_with_id(self, data):
        data['client_id'] = self.id
        self.master.broadcast_to_slaves(data)

    def get_hostname(self):
        if 'hostname' in self.last_beat:
            return self.last_beat['hostname']
        else:
            log.error('Attempt to get hostname before heartbeat')

    def get_sink_host(self):
        hostname = self.get_hostname()
        if hostname:
            return '{}:{}'.format(hostname, SLAVE_SINK_PORT)
        else:
            log.error('Attempt to get_sink_host before hostname heartbeat')

    @action
    def connected_to_sink(self, data):
        if 'sink_id' not in data:
            return {'error': 'id or sink_id not specified'}
        sink_id = data['sink_id']

        if sink_id in self.master.slave_registry:
            self.sink = self.master.slave_registry[sink_id]
            self.master.register_sink(self.sink)
            self.master.clients.slave_set_sink({
                'slave_id': self.id,
                'sink_id': sink_id
            })
            log.info('Slave ({}) has connected to sink ({})'.format(self.id, data['sink_id']))
        else:
            log.warn('Sink {} not found in registry'.format(data['sink_id']))

    @action
    def heartbeat(self, data):
        self.last_beat = data
        self.master.clients.slave_heartbeat(data)

    @action
    def quit(self, data):
        self.master.remove_slave(self.id)

    @action
    def test_started(self, data):
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].send.test_running()
        else:
            log.warn('Client id not in test_started')

    @action
    def test_result(self, data):
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].send.test_result(data)
            else:
                log.warn('Test result for client({}) cannot be sent, client not found'.format(client_id))
        else:
            log.warn('Client id not in test_result')
            return {'error': 'Please set client_id'}

    @action
    def test_finished(self, data):
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].send.test_stopped()


@action_class
class Client(Actionable):
    def __init__(self, master, websocket, id):
        self.master = master
        self.websocket = websocket
        self.id = id
        self.send = Sender(self.send_data)

    def send_data(self, data):
        self.websocket.send(json.dumps(data))

    def handle_websocket_message(self, msg):
        if msg:
            data = json.loads(msg)
            if 'action' in data:
                self.run_action(data)

    @action
    def get_id(self, data):
        self.send.set_id({'client_id': self.id})

    @action
    def quit(self, data):
        if 'slave_id' in data:
            self.master.remove_slave(data['slave_id'])
        else:
            self.send.error({'error': 'Id not specified in data'})

    @action
    def request_slaves(self, data):
        data = {'slaves': dict((slave.id, slave.last_beat)
                               for slave in self.master.slave_registry.values())}
        self.send.receive_slaves(data)

    @action
    def set_sink(self, data):
        if 'sink_id' not in data or 'slave_id' not in data:
            raise Exception('Missing sink_id or slave_id')

        self.master.set_sink(data['slave_id'], data['sink_id'], self)

    @action
    def request_available_tests(self, data):
        tests_glob = os.path.join(os.path.dirname(__file__), TEST_DIR, '*.json')
        files = glob.glob(tests_glob)
        files_stripped = [os.path.basename(filename)[:-5] for filename in files]
        self.send.receive_available_tests({'tests': files_stripped})

    @action
    def request_test(self, data):
        filename = os.path.join(os.path.dirname(__file__), TEST_DIR, data['name'] + '.json')
        if os.path.exists(filename):
            with open(filename) as file:
                return self.send.receive_test({'test': json.loads(file.read())})
        return self.send.error({'error': 'Test not found'})

    @action
    def save_test(self, data):
        if 'test' in data:
            test = data['test']
            if 'name' in test:
                filename = os.path.join(os.path.dirname(__file__), TEST_DIR, test['name'] + '.json')
                with open(filename, 'w') as file:
                    file.write(json.dumps(test))
                self.send.save_successful()

    @action
    def delete_test(self, data):
        if 'test_name' in data:
            filename = os.path.join(os.path.dirname(__file__), TEST_DIR, data['test_name'] + '.json')
            if os.path.exists(filename):
                os.remove(filename)
            else:
                return self.send.error({'error': 'Test not found'})
        else:
            return self.send.error({'error': 'Cannot delete test, not found'})

    @action
    def run_test(self, data):
        if 'test' in data:
            data['client_id'] = self.id
            if 'runs' in data['test']:
                try:
                    data['runs'] = int(data['test']['runs'])/len(self.master.slave_registry)
                except ValueError:
                    data['runs'] = 1
            else:
                data['runs'] = 1
            self.master.slaves.run_test(data)

    @action
    def stop_test(self, data):
        data['client_id'] = self.id
        if len(self.master.slave_registry) != 0:
            self.master.slaves.stop_test(data)
        else:
            self.send.test_stopped()

if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()
    master_app = MasterApplication(context)
    gevent.spawn(master_app.watch_slaves)
    WebSocketServer(('', WEBSOCKET_PORT), master_app).start()
    # Interface
    dev_null = open('/dev/null', 'w')
    gevent.wsgi.WSGIServer(('', HTTP_PORT), paste.urlparser.StaticURLParser(os.path.dirname(__file__)), log=dev_null).serve_forever()

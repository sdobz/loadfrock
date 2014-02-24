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
        print('Master initialized')
        self.context = context
        self.slave_in = context.socket(zmq.REP)
        self.slave_in.bind("tcp://*:{}".format(SLAVE_REP_PORT))
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
        print('Websocket {} connected!'.format(client.id))

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
        print('Listening to slaves')
        while True:
            msg = self.slave_in.recv()
            resp = self.handle_slave_message(msg)
            self.slave_in.send(resp)

    def handle_slave_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            slave, assign_id = self.load_slave(data)
            if assign_id:
                data['slave_id'] = assign_id
            resp = slave.run_action(data) or {}
            if assign_id:
                resp['assign_id'] = assign_id
        else:
            resp = {'error': 'No action specified'}
        return bson.dumps(resp)

    def load_slave(self, data):
        if 'slave_id' not in data or data['slave_id'] not in self.slave_registry:
            slave = self.new_slave()
            return slave, slave.id

        return self.slave_registry[data['slave_id']], None

    def new_slave(self):
        slave = Slave(self, self.get_next_slave_id())
        self.slave_registry[slave.id] = slave
        self.clients.slave_heartbeat({'slave_id': slave.id})
        return slave

    def get_next_slave_id(self):
        self.next_slave_id += 1
        return self.next_slave_id

    def pick_sink(self, slave):
        hostname = slave.last_beat.get('hostname')
        if hostname and hostname not in self.sink_registry:
            return slave
        else:
            return self.sink_registry[hostname]

    def remove_slave(self, id):
        if id in self.slave_registry:
            print('Killing slave {}'.format(id))
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

    def get_sink_host(self):
        hostname = self.last_beat.get('hostname')
        if hostname:
            return '{}:{}'.format(hostname, SLAVE_SINK_PORT)

    @action
    def connected_to_sink(self, data):
        if 'sink_id' not in data:
            return {'error': 'id or sink_id not specified'}
        sink_id = data['sink_id']

        if sink_id in self.master.slave_registry:
            print('Slave ({}) has connected to sink ({})'.format(self.id, data['sink_id']))

            self.sink = self.master.slave_registry[sink_id]
            self.master.sink_registry[self.last_beat['hostname']] = self.sink
            self.master.clients.slave_set_sink({
                'slave_id': self.id,
                'sink_id': sink_id
            })

    @action
    def heartbeat(self, data):
        self.last_beat = data
        self.master.clients.slave_heartbeat(data)

    @action
    def quit(self, data):
        self.master.remove_slave(self.id)

    @action
    def test_result(self, data):
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].test_result(data)
            else:
                print('Test result for client({}) cannot be sent, client not found'.format(client_id))
        else:
            return {'error': 'Please set client_id'}


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

        sink_id = data['sink_id']
        if sink_id not in self.master.slave_registry:
            self.send.error({'error': 'Sink({}) not found'.format(sink_id)})

        sink_host = self.master.slave_registry[sink_id].get_sink_host()
        if not sink_host:
            self.send.error({'error': 'Sink({}) does not have a host yet'.format(sink_id)})

        data['sink_host'] = sink_host
        del data['slave_id']

        self.master.slaves.set_sink(data)

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
            self.send.test_running()

    @action
    def stop_test(self, data):
        data['client_id'] = self.id
        self.master.slaves.stop_test(data)
        self.send.test_stopped()

if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()
    master_app = MasterApplication(context)
    gevent.spawn(master_app.watch_slaves)
    WebSocketServer(('', WEBSOCKET_PORT), master_app).start()
    gevent.wsgi.WSGIServer(
        ('', HTTP_PORT), paste.urlparser.StaticURLParser(os.path.dirname(__file__))).serve_forever()

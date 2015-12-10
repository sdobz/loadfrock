from py.shared import Actionable, Sender, action, action_class
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
    """
    This class represents the master controlling server. It tracks slaves, sinks, and clients.
    """
    slave_registry = {}
    sink_registry = {}
    next_slave_id = 0
    next_client_id = 0
    client_registry = {}
    clients = Sender(None)
    removed_slaves = set()

    def __init__(self, context):
        log.info('Master initialized')
        # wsgi context, unused:
        self.context = context

        # Slaves send data in via this socket
        self.slave_in = context.socket(zmq.PULL)
        self.slave_in.bind("tcp://*:{}".format(SLAVE_MASTER_PORT))
        # listen_to_slaves handles any incoming messages
        gevent.spawn(self.listen_to_slaves)

        # This broadcasts to all slaves simultaneously.
        # TODO: Selective slave message sending
        self.slave_out = context.socket(zmq.PUB)
        self.slave_out.bind("tcp://*:{}".format(SLAVE_PUB_PORT))

        # What a clever boy I am (clever code is bad code)
        # Allow self.slaves.<action>(data) magic
        self.slaves = Sender(self.broadcast_data_to_slaves)
        # Same for clients (where client is a browser connected via websocket)
        self.clients = Sender(self.broadcast_data_to_clients)

    def broadcast_data_to_slaves(self, data):
        """
        Send data to all slaves as a bson dump
        :param data: Data to serialize
        """
        self.slave_out.send(bson.dumps(data))

    def broadcast_data_to_clients(self, data):
        """
        Send data to all web clients (in serial) as a json dump
        :param data: Data to serialize
        """
        # TODO: parallelize
        for client in self.client_registry.values():
            client.send_data(data)

    def __call__(self, environ, start_response):
        """
        This class is called whenever a browser tries to connect.
        Note: This is green-threaded via gevent
        :param environ: The wsgi environment
        :param start_response: A helper to return http responses
        :return:
        """
        # Respond to any websocket messages
        ws = environ['wsgi.websocket']
        self.listen_to_websocket(ws)

    def listen_to_websocket(self, ws):
        """
        Listen to incoming messages on the websocket and delegate them to a message handler.
        :param ws: Websocket to listen to
        """
        client = self.add_client(ws)
        log.info('Websocket {} connected!'.format(client.id))

        # Listen for messages until they disconnect
        while True:
            try:
                msg = ws.receive()
                client.handle_websocket_message(msg)
            except WebSocketError as e:
                self.disconnect_client(client)
                break

    def get_next_client_id(self):
        """
        :return: an incrementing id to identify clients
        """
        self.next_client_id += 1
        return self.next_client_id - 1

    def add_client(self, ws):
        """
        Create a new Client instance and store it in the client registry
        :param ws: Websocket to associate with the client so the instance can send messages
        :return: The client instance
        :rtype Client:
        """
        id = self.get_next_client_id()
        client = Client(self, ws, id)
        self.client_registry[id] = client
        return client

    def disconnect_client(self, client):
        """
        Close the connection to this specific client and deregister them
        :param client: Client to disconnect
        """
        if client.id in self.client_registry:
            client.websocket.close()
            del self.client_registry[client.id]

    def listen_to_slaves(self):
        """
        Delegate any messages received from the slaves to the proper handler
        :return:
        """
        log.info('Listening to slaves')
        while True:
            self.handle_slave_message(self.slave_in.recv())

    def handle_slave_message(self, msg):
        """
        Slaves send in heartbeats and test results. Handle finding the associated local slave instance and interpreting
        the message.
        :param msg: A bson encoded dict with message data.
            action:   A string representing the action being taken. Must have an associated function on the local slave class
                      with the @action decorator
            slave_id: The id assigned to the slave. If this is not present the slave doesn't have one and should be sent one
            <message specific data>
        """
        # TODO: Think through the security implications of letting slaves report their own ids and uuids
        data = bson.loads(msg)
        if 'action' in data:
            # Slaves without a valid slave_id are unregistered and need to be created locally
            # TODO: Move slave creation into load_slave
            if 'slave_id' not in data or data['slave_id'] not in self.slave_registry:
                if data['action'] == 'heartbeat':
                    # Heartbeats are typically the way slaves are discovered
                    slave = self.new_slave(data)
                    # TODO: handle slave creation failure
                else:
                    log.error('Got non-heartbeat message from unknown slave')
                    return
            else:
                # Load the local instance
                slave = self.load_slave(data)
                # TODO: handle save loading failure

            assert slave
            # run_action looks up the @action method on the slave and runs it with data
            slave.run_action(data)

    def load_slave(self, data):
        """
        Inspect the message data and retrieve the associated slave
        :param data: Message data
        :return: The loaded slave
        :rtype Slave:
        """
        if 'slave_id' not in data:
            log.warn('load_slave without slave_id')
            return
        slave_id = data['slave_id']
        if slave_id not in self.slave_registry:
            log.warn("load_slave couldn't find slave")
            return
        return self.slave_registry[slave_id]

    def new_slave(self, heartbeat_data):
        """
        Create a slave from its heartbeat
        :param heartbeat_data:
        :return: The created Slave
        :rtype Slave:
        """
        slave = Slave(self, self.get_next_slave_id())
        # Modify the data, providing a slave_id to run_action if it needs it
        heartbeat_data['slave_id'] = slave.id

        # Kinda a hack, we want to get the sink_host before the action is run,
        # and that requires heartbeat data, so we store it if we have it
        slave.last_beat = heartbeat_data

        # Broadcast a message to all slaves associating that uuid with the local slave_id
        self.slaves.set_id({
            'new_slave_id': slave.id,
            'slave_uuid': heartbeat_data['slave_uuid']
        })

        # Slaves send test data to the sink, and the sink collates it before sending it to the server.
        # There is one sink per unique hostname
        sink = self.pick_sink(slave)
        # TODO: handle if no sink
        assert sink

        # Tell the new slave what sink to use
        self.slaves.set_sink({
            'sink_id': sink.id,
            'sink_host': sink.get_sink_host()
        })

        # Register the slave so it can be found again
        self.slave_registry[slave.id] = slave

        return slave

    def get_next_slave_id(self):
        """
        :return: An auto incrementing id to identify slaves
        """
        self.next_slave_id += 1
        return self.next_slave_id

    def pick_sink(self, slave):
        """
        Pick a sink on the same host as the given slave
        :param slave: Slave to find a sink for
        :return:
        """
        hostname = slave.get_hostname()
        if hostname:
            # If it is a new host then it is the sink!
            if hostname not in self.sink_registry:
                return slave
            else:
                # otherwise look up the hostname
                return self.sink_registry[hostname]

    def set_sink(self, slave_id, sink_id, client=None):
        """
        Forcefully set the sink for all slaves on a host.
        :param slave_id: Unused
        :param sink_id: The slave_id to promote to a sink
        :param client: The client that requested the sink change
        """
        # TODO: Move client error reporting into the client class

        if sink_id not in self.slave_registry:
            log.warn('set_sink to unknown sink')
            if client:
                client.send.error({'error': 'Sink({}) not found'.format(sink_id)})
            return

        # Look up the hostname for the promoted sink
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

        # Tell all slaves with the host <sink_host> to connect to the given sink
        self.slaves.set_sink(data)

    def register_sink(self, sink):
        """
        Record when a slave connects to a sink
        :param sink: Sink that has been connected to
        """
        self.sink_registry[sink.get_hostname()] = sink

    def remove_slave(self, id):
        """
        Remove, disconnect, and shut down the given slave id
        :param id: Slave to remove
        """
        if id in self.slave_registry:
            log.info('Killing slave {}'.format(id))
            del self.slave_registry[id]
        # Any slave that reports to that id will quit
        # TODO: switch to slave.broadcast_to.quit()
        self.slaves.quit({'slave_id': id})
        self.clients.slave_disconnected({'slave_id': id})

    def check_slaves(self):
        """
        Iterate through all slaves and check the last time they heartbeat
        If it is too long ago remove them
        :return:
        """
        the_time = int(time())
        # Since we modify the dict (and that messes with iteration) first get a static list of all of them
        slave_ids = self.slave_registry.keys()
        for slave_id in slave_ids:
            slave = self.slave_registry[slave_id]
            if the_time - slave.last_beat.get('generated', 0) > HEARTBEAT_PERIOD * BEATS_TO_KILL:
                self.remove_slave(slave_id)

    def watch_slaves(self):
        """
        This will periodically check slaves
        :return:
        """
        while True:
            self.check_slaves()
            gevent.sleep(HEARTBEAT_PERIOD)

@action_class
class Slave(Actionable):
    """
    This class is the local representation of a remote slave. When the remote slave sends in a message the master routes
    it into the proper local Slave instance for handling.
    Depending on the "action" key of the message is which method will be called
    """
    sink = None

    def __init__(self, master, id):
        self.master = master
        self.last_beat = {}
        self.id = id

        # This syntax allows:
        # slave_instance.broadcast_to.<action>(data)
        # Since the master can only broadcast to all slaves this attaches the slave id to the message so only the
        # correct slave interprets it
        self.broadcast_to = Sender(self.annotate_broadcast_with_id)

    def annotate_broadcast_with_id(self, data):
        """
        Attach the slaves id to the message data and send it
        :param data: Message data to modify
        :return:
        """
        # add id
        data['slave_id'] = self.id
        # Send to all slaves
        self.master.broadcast_to_slaves(data)

    def get_hostname(self):
        """
        The hostname is only available after a heartbeat
        :return: The slaves last reported hostname
        """
        if 'hostname' in self.last_beat:
            return self.last_beat['hostname']
        else:
            log.error('Attempt to get hostname before heartbeat')

    def get_sink_host(self):
        """
        Get the full host including port
        :return: The hostname:port combo
        """
        hostname = self.get_hostname()
        if hostname:
            return '{}:{}'.format(hostname, SLAVE_SINK_PORT)
        else:
            log.error('Attempt to get_sink_host before hostname heartbeat')

    # TODO: Separate slave and sink methods
    # SLAVE
    @action
    def connected_to_sink(self, data):
        """
        When a slave connects to a sink it reports to the master
        :param data: Action specific data:
            sink_id: Slave+sink id connected
        :return:
        """
        if 'sink_id' not in data:
            return {'error': 'id or sink_id not specified'}
        sink_id = data['sink_id']

        if sink_id in self.master.slave_registry:
            # Store the local slave instance that represents the remote sink/slave
            self.sink = self.master.slave_registry[sink_id]
            self.master.register_sink(self.sink)
            # Report to clients that this slave connected to that sink
            self.master.clients.slave_set_sink({
                'slave_id': self.id,
                'sink_id': sink_id
            })
            log.info('Slave ({}) has connected to sink ({})'.format(self.id, data['sink_id']))
        else:
            log.warn('Sink {} not found in registry'.format(data['sink_id']))

    # SLAVE
    @action
    def heartbeat(self, data):
        """
        Receive heartbeat data
        :param data: Message specific data:
            hostname: Slave hostname
            memory: Memory usage
                physical: Physical usage
                virtual: Virtual usage
                swap: Swap usage
            load: System load (1s)
            bandwidth:
                in: [bytes/s, total bytes]
                out: [bytes/s, total bytes]
            generated: Unix timestamp message was generated
            sink_id: Slave id connected to as sink
            slave_uuid: UUID of slave
        """
        self.last_beat = data
        # Report heartbeat data to clients
        self.master.clients.slave_heartbeat(data)

    # SLAVE
    @action
    def quit(self, data):
        """
        Report that the slave has quit
        :param data:
        """
        self.master.remove_slave(self.id)

    # SINK
    @action
    def test_started(self, data):
        """
        Report that a test has started to a specific client
        :param data: Message specific data:
            client_id: Client that test has started for
        """
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                # Report to the correct client that the test has started
                self.master.client_registry[client_id].send.test_running()
        else:
            log.warn('Client id not in test_started')

    # SINK
    @action
    def test_result(self, data):
        """
        Receive results from a sink and report it to clients
        :param data:
        """
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].send.test_result(data)
            else:
                log.warn('Test result for client({}) cannot be sent, client not found'.format(client_id))
        else:
            log.warn('Client id not in test_result')
            return {'error': 'Please set client_id'}

    # SINK
    @action
    def test_finished(self, data):
        """
        Report that a test has finished
        :param data:
        :return:
        """
        if 'client_id' in data:
            client_id = data['client_id']
            if client_id in self.master.client_registry:
                self.master.client_registry[client_id].send.test_stopped()


@action_class
class Client(Actionable):
    """
    This class is the local representation of a remote client (browser)
    When a websocket message is received it is routed into the proper local instance
    When the master sends a message to a client it goes through this class
    """
    def __init__(self, master, websocket, id):
        self.master = master
        self.websocket = websocket
        self.id = id
        # This is syntax for sending message through a websocket.
        # client.send.<action>(data)
        self.send = Sender(self.send_data)

    def send_data(self, data):
        self.websocket.send(json.dumps(data))

    def handle_websocket_message(self, msg):
        """
        This handles any message that comes in by looking at the action key and seeing if it is a local method,
        then calling it
        :param msg: JSON encoded data from the websocket
        """
        if msg:
            data = json.loads(msg)
            if 'action' in data:
                self.run_action(data)

    @action
    def get_id(self, data):
        self.send.set_id({'client_id': self.id})

    @action
    def quit(self, data):
        """
        Websocket telling a slave to quit
        :param data: Message specific data:
            slave_id: slave to tell to quit
        """
        if 'slave_id' in data:
            self.master.remove_slave(data['slave_id'])
        else:
            self.send.error({'error': 'Id not specified in data'})

    @action
    def request_slaves(self, data):
        """
        Websocket requesting slave data
        """
        data = {'slaves': dict((slave.id, slave.last_beat)
                               for slave in self.master.slave_registry.values())}

        # Send a message with action=receive_slaves to the browser
        self.send.receive_slaves(data)

    @action
    def set_sink(self, data):
        """
        Set the sink on a particular slave, which sets the sink for all slaves on that host
        :param data: message specific data:
            slave_id: Slave to set the sink for (unused)
            sink_id: Slave to set the sink to
        """
        if 'sink_id' not in data or 'slave_id' not in data:
            raise Exception('Missing sink_id or slave_id')

        # Let the master handle actually doing this
        self.master.set_sink(data['slave_id'], data['sink_id'], self)

    @action
    def request_available_tests(self, data):
        """
        Client asking to load tests
        :param data:
        """
        tests_glob = os.path.join(os.path.dirname(__file__), TEST_DIR, '*.json')
        files = glob.glob(tests_glob)
        files_stripped = [os.path.basename(filename)[:-5] for filename in files]
        # Send list of files to the client
        self.send.receive_available_tests({'tests': files_stripped})

    @action
    def request_test(self, data):
        """
        Client asking to load a test
        :param data: Message specific data:
            name: filename to load
        """
        filename = os.path.join(os.path.dirname(__file__), TEST_DIR, data['name'] + '.json')
        # TODO: Prevent path traversal
        if os.path.exists(filename):
            with open(filename) as file:
                return self.send.receive_test({'test': json.loads(file.read())})
        return self.send.error({'error': 'Test not found'})

    @action
    def save_test(self, data):
        """
        Save a test to a file
        :param data: Message specific data:
            name: Filename to save to
            test: data representing a test
        """
        if 'test' in data:
            test = data['test']
            if 'name' in test:
                # TODO: Prevent path traversal
                filename = os.path.join(os.path.dirname(__file__), TEST_DIR, test['name'] + '.json')
                with open(filename, 'w') as file:
                    file.write(json.dumps(test))
                self.send.save_successful()

    @action
    def delete_test(self, data):
        """
        Delete a test
        :param data: Message specific data:
            test_name: Name of test to delete
        """
        if 'test_name' in data:
            # TODO: Prevent path traversal
            filename = os.path.join(os.path.dirname(__file__), TEST_DIR, data['test_name'] + '.json')
            if os.path.exists(filename):
                os.remove(filename)
            else:
                return self.send.error({'error': 'Test not found'})
        else:
            return self.send.error({'error': 'Cannot delete test, not found'})

    @action
    def run_test(self, data):
        """
        Send a test to all slaves to run
        :param data: Message specific data:
            test: Test data to run
        """
        if 'test' in data:
            data['client_id'] = self.id
            if 'runs' in data['test']:
                try:
                    # Each slave gets a portion of the total runs
                    data['runs'] = int(data['test']['runs'])/len(self.master.slave_registry)
                except ValueError:
                    data['runs'] = 1
            else:
                data['runs'] = 1
            self.master.slaves.run_test(data)

    @action
    def stop_test(self, data):
        """
        Halt all tests run by this client
        :param data:
        """
        data['client_id'] = self.id
        if len(self.master.slave_registry) != 0:
            self.master.slaves.stop_test(data)
        else:
            self.send.test_stopped()

if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()
    master_app = MasterApplication(context)
    # Spawn the slave watchdog responsible for pruning lost slaves
    gevent.spawn(master_app.watch_slaves)
    # Spawn the websocket server serving the client RPC
    WebSocketServer(('', WEBSOCKET_PORT), master_app).start()
    # Interface
    dev_null = open('/dev/null', 'w')
    # Spawn the file server serving static assets
    gevent.wsgi.WSGIServer(('', HTTP_PORT), paste.urlparser.StaticURLParser(os.path.dirname(__file__)), log=dev_null).serve_forever()

from py.shared import *
from config import *
from time import time
from zmq.error import ZMQError
from gevent.lock import BoundedSemaphore
import zmq.green as zmq
import bson
import psutil
import gevent
import sys
import socket


@action_class
class Slave(Actionable):
    id = None
    last_bw_out = None
    bw_out = 0
    total_bw_out = 0
    last_bw_in = None
    bw_in = 0
    total_bw_in = 0
    running_tests = []
    test_runner = None
    sink_obj = None
    sink = None
    message_lock = BoundedSemaphore(1)

    def __init__(self, context):
        self.context = context
        self.socket_out = context.socket(zmq.REQ)
        self.socket_out.connect("tcp://localhost:{}".format(SLAVE_REP_PORT))
        self.master = Sender(self.send_to_master)

        self.socket_in = context.socket(zmq.SUB)
        self.socket_in.setsockopt(zmq.SUBSCRIBE, "")
        self.socket_in.connect("tcp://localhost:{}".format(SLAVE_PUB_PORT))

        self.sink_host = '{}:{}'.format(self.hostname(), SLAVE_SINK_PORT)

    def send_to_master(self, data):
        if self.id:
            data['slave_id'] = self.id
        with Slave.message_lock:
            self.socket_out.send(bson.dumps(data))
            resp = bson.loads(self.socket_out.recv())
        self.check_for_id(resp)
        return resp

    def send_to_sink(self, data):
        if self.id:
            data['slave_id'] = self.id
        self.sink_obj.send_to_sink(data)

    def check_for_id(self, resp):
        if 'assign_id' in resp:
            self.id = resp['assign_id']
            print('Assigned id: {}'.format(self.id))

    def listen_to_master(self):
        print("Listening to master")
        while True:
            self.handle_master_message(self.socket_in.recv())

    def handle_master_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            if 'slave_id' not in data or data['slave_id'] == self.id:
                self.run_action(data)
        else:
            print('Server sent message with no action')

    @staticmethod
    def hostname():
        return socket.gethostname()

    def connect_to_sink(self, sink_id, host):
        print('Attempting to connect to sink({}) - {}'.format(sink_id, host))
        if self.sink_obj:
            self.sink_obj.close()
        self.sink_obj = Sink(id=sink_id,
                             host=host,
                             slave=self)

        if self.sink_obj.setup():
            print('Ok!')
            self.sink = Sender(self.send_to_sink)
            self.master.connected_to_sink({
                'sink_id': self.sink_obj.id
            })

    def heartbeat(self):
        physical_memory = psutil.phymem_usage()
        virtual_memory = psutil.virtual_memory()
        swap_memory = psutil.swap_memory()
        load = psutil.cpu_percent(interval=1)
        network = psutil.net_io_counters()
        hostname = self.hostname()

        if not self.last_bw_out:
            self.last_bw_out = network.bytes_sent
        self.bw_out = (network.bytes_sent - self.last_bw_out)/HEARTBEAT_PERIOD
        self.total_bw_out += self.bw_out * HEARTBEAT_PERIOD
        self.last_bw_out = network.bytes_sent

        if not self.last_bw_in:
            self.last_bw_in = network.bytes_recv
        self.bw_in = (network.bytes_recv - self.last_bw_in)/HEARTBEAT_PERIOD
        self.total_bw_in += self.bw_in * HEARTBEAT_PERIOD
        self.last_bw_in = network.bytes_recv

        self.master.heartbeat({
            'hostname': hostname,
            'memory': {
                'physical': (physical_memory.used, physical_memory.total),
                'virtual': (virtual_memory.used, virtual_memory.total),
                'swap': (swap_memory.used, swap_memory.total)
            },
            'load': load,
            'bandwidth': {
                'in': (self.bw_in, self.total_bw_in),
                'out': (self.bw_out, self.total_bw_out)
            },
            'generated': int(time()),
            'sink_id': self.sink_obj.id if self.sink_obj else None
        })

    def heartbeat_forever(self):
        print('Beating heart forever')
        while True:
            self.heartbeat()
            gevent.sleep(HEARTBEAT_PERIOD)

    def async_test(self, data):
        print('Starting async test')
        client_id = data['client_id']
        if not self.sink_obj:
            print('Error, no sink')
            return self.test_error(client_id, 'Slave({}) has no sink'.format(self.id))

        context = {}
        runs = data.get('runs', 1)
        for test_num in xrange(0, runs):
            print('Eunning test')
            self.sink.test_result({'client_id': client_id,
                                        'slave_id': self.id,
                                        'message': 'Ran test {}'.format(test_num)})
            gevent.sleep(.1)

    def test_error(self, client_id, error_str):
        self.master.test_result({'id': client_id, 'error': error_str})

    @action
    def set_sink(self, data):
        if 'sink_id' in data and 'sink_host' in data and data['sink_host'] == self.sink_host:
            self.connect_to_sink(data['sink_id'], data['sink_host'])

    @action
    def quit(self, data):
        print('Master told us to quit, quitting.')
        sys.exit(0)

    @action
    def run_test(self, data):
        if 'test' in data:
            gevent.spawn(self.async_test, data)

        else:
            raise Exception('No test given')


class TestRunner:
    def __init__(self, context):
        pass

    @staticmethod
    def safe_eval(code, context=None):
        glob = {'__builtins__': None}
        if context:
            glob.update(context)
        return eval(code, glob)

@action_class
class Sink(Actionable):
    sock = None
    is_host = None
    connection_prefix = 'tcp://'
    listen_loop = None
    socket_lock = BoundedSemaphore(1)

    def __init__(self, id, host, slave):
        self.id = id
        self.host = host
        self.slave = slave
        self.master = Sender(self.send_to_master)

    def send_to_master(self, data):
        data['sink_id'] = self.id
        self.slave.send_to_master(data)

    def send_to_sink(self, data):
        # If we're not the host send it
        data['sink_id'] = self.id
        if not self.is_host:
            self.sock.send(bson.dumps(data))
        else:
            # We're the host, so just handle it
            self.handle_sink_message(data)

    def setup(self):
        self.is_host = self.id == self.slave.id
        self.create_socket(zmq.PULL if self.is_host else zmq.PUSH)

        for attempt in xrange(1, 4):
            try:
                if self.is_host:
                    self.sock.bind(self.connection_prefix + '*:{}'.format(SLAVE_SINK_PORT))
                    self.listen_loop = gevent.spawn(self.listen_to_slaves)
                else:
                    self.sock.connect(self.connection_prefix + self.host)
                return True
            except ZMQError:
                if attempt == 1:
                    print('Sink({}) failed to {} to {}'.format(
                        self.id,
                        'bind' if self.is_host else 'connect',
                        self.host))
                print('Attempt {} of 3 failed...'.format(attempt))
                gevent.sleep(1)
        print('Failed to connect')
        return False

    def create_socket(self, type):
        with Sink.socket_lock:
            self.close()
            self.sock = self.slave.context.socket(type)

    def close(self):
        if self.sock:
            self.sock.close()

    def listen_to_slaves(self):
        print('Sink listening')
        while True:
            try:
                self.handle_sink_message(bson.loads(self.sock.recv()))
            except ZMQError:
                print('Sink stopped listening')
                continue

    def handle_sink_message(self, data):
        if 'action' in data:
            self.run_action(data)
        else:
            print('Slave sent message to sink with no action')

    @action
    def test_result(self, data):
        print('Sink({}) got test result'.format(self.id), data)



if __name__ == "__main__":
    try:
        context = zmq.Context()
        slave = Slave(context)
        gevent.spawn(slave.heartbeat_forever)
        slave.listen_to_master()
    except KeyboardInterrupt:
        print('Telling master we quit')
        slave.master.quit({'id': slave.id})
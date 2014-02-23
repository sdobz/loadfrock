from py.shared import *
from py.action_wrapper import MasterActionWrapper
from config import *
from time import time
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
    sink = None

    def __init__(self, context):
        self.socket_out = context.socket(zmq.REQ)
        self.socket_out.connect("tcp://localhost:{}".format(SLAVE_REP_PORT))
        self.master = MasterActionWrapper(self.socket_out)

        self.socket_in = context.socket(zmq.SUB)
        self.socket_in.setsockopt(zmq.SUBSCRIBE, "")
        self.socket_in.connect("tcp://localhost:{}".format(SLAVE_PUB_PORT))

    def listen_to_master(self):
        print("Listening to master")
        while True:
            self.handle_master_message(self.socket_in.recv())

    def handle_master_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            self.run_action(data)
        else:
            print('Server sent message with no action')

    def connect(self):
        self.id = self.master.connect()['id']
        print("Slave connected, id: {}".format(self.id))

    def heartbeat(self):
        physical_memory = psutil.phymem_usage()
        virtual_memory = psutil.virtual_memory()
        swap_memory = psutil.swap_memory()
        load = psutil.cpu_percent(interval=1)
        network = psutil.net_io_counters()
        hostname = socket.gethostname()

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
            'id': self.id,
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
        })

    def heartbeat_forever(self):
        print('Beating heart forever')
        while True:
            self.heartbeat()
            gevent.sleep(HEARTBEAT_PERIOD)

    def async_test(self, data):
        print('Starting async test')
        client_id = data['id']
        context = {}
        runs = data.get('runs', 1)
        for test_num in xrange(0, runs):
            self.master.test_result({'id': client_id,
                                     'slave_id': self.id,
                                     'message': 'Ran test {}'.format(test_num)})
            gevent.sleep(.1)

    def test_error(self, client_id, error_str):
        self.master.test_result({'id': client_id, 'error': error_str})

    @action
    def quit(self, data):
        if 'id' not in data or data['id'] == self.id:
            print('Master told us to quit, quitting.')
            sys.exit(0)

    @action
    def run_test(self, data):
        if 'id' in data:
            gevent.spawn(self.async_test, data)


class TestRunner:
    def __init__(self, context):
        pass

    @staticmethod
    def safe_eval(code, context=None):
        glob = {'__builtins__': None}
        if context:
            glob.update(context)
        return eval(code, glob)

class Sink:
    pass


if __name__ == "__main__":
    try:
        context = zmq.Context()
        slave = Slave(context)
        slave.connect()
        gevent.spawn(slave.heartbeat_forever)
        slave.listen_to_master()
    except KeyboardInterrupt:
        print('Telling master we quit')
        slave.master.quit({'id': slave.id})
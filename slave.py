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
import random
import grequests
from requests import Session
from uuid import uuid4
import re
import logging
log = logging.getLogger('slave')
logging.basicConfig(level=logging.INFO)


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
    client_tests = {}
    message_lock = BoundedSemaphore(1)

    def __init__(self, context):
        self.context = context
        self.socket_out = context.socket(zmq.PUSH)
        self.socket_out.connect("tcp://localhost:{}".format(SLAVE_MASTER_PORT))
        self.master = Sender(self.send_to_master)

        self.socket_in = context.socket(zmq.SUB)
        self.socket_in.setsockopt(zmq.SUBSCRIBE, "")
        self.socket_in.connect("tcp://localhost:{}".format(SLAVE_PUB_PORT))

        self.sink_host = '{}:{}'.format(self.hostname(), SLAVE_SINK_PORT)
        self.uuid = uuid4().hex

    def send_to_master(self, data):
        if self.id:
            data['slave_id'] = self.id
        if data['action'] != 'heartbeat':
            log.debug('MASTER->{}'.format(data['action']))
        self.socket_out.send(bson.dumps(data))
        if data['action'] != 'heartbeat':
            log.debug('MASTER->{} end'.format(data['action']))

    def send_to_sink(self, data):
        if self.id:
            data['slave_id'] = self.id
        if self.sink_obj:
            self.sink_obj.send_to_sink(data)
        else:
            log.warn('{}->SINK failed, no sink'.format(data['action']))

    def check_for_props(self, resp):
        if 'assign_slave_id' in resp:
            self.id = resp['assign_slave_id']
            log.info('Assigned id: {}'.format(self.id))
        if 'assign_sink_id' in resp:
            if 'assign_sink_host' not in resp:
                log.warn('assign_sink_id without host')
            else:
                self.connect_to_sink(resp['assign_sink_id'],
                                     resp['assign_sink_host'])

    def listen_to_master(self):
        log.info("Listening to master")
        while True:
            self.handle_master_message(self.socket_in.recv())

    def handle_master_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            if 'slave_id' not in data or data['slave_id'] == self.id:
                log.debug('{}<-MASTER'.format(data['action']))
                self.run_action(data)
                log.debug('{}<-MASTER end'.format(data['action']))
        else:
            log.warn('Server sent message with no action')

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
            'sink_id': self.sink_obj.id if self.sink_obj else None,
            'slave_uuid': self.uuid
        })

    def heartbeat_forever(self):
        log.info('Beating heart forever')
        while True:
            self.heartbeat()
            gevent.sleep(HEARTBEAT_PERIOD)

    @action
    def set_id(self, data):
        if 'slave_uuid' not in data or 'new_slave_id' not in data:
            log.warn('receiving id without slave_uuid/new_slave_id')
            return
        if data['slave_uuid'] == self.uuid:
            self.id = data['new_slave_id']
            log.info('Received id: {}'.format(self.id))

    @action
    def set_sink(self, data):
        if 'sink_id' in data and 'sink_host' in data and data['sink_host'] == self.sink_host:
            self.connect_to_sink(data['sink_id'], data['sink_host'])
        else:
            log.warn('Incomplete sink call')

    @staticmethod
    def hostname():
        return socket.gethostname()

    def connect_to_sink(self, sink_id, host):
        log.info('Attempting to connect to sink({}) - {}'.format(sink_id, host))
        if self.sink_obj:
            log.info('Closing previous sink')
            self.sink_obj.close()
        self.sink_obj = Sink(id=sink_id,
                             host=host,
                             slave=self)

        if self.sink_obj.setup():
            log.info('Ok!')
            self.sink = Sender(self.send_to_sink)
            self.master.connected_to_sink({
                'sink_id': self.sink_obj.id
            })

    @action
    def quit(self, data):
        log.info('Master told us to quit, quitting.')
        sys.exit(0)

    @action
    def run_test(self, data):
        log.info('slave({}).run_test client({})'.format(self.id, data['client_id']))
        if not self.sink_obj:
            log.warn('Error, no sink')
            return
        if 'test' in data:
            client_id = data['client_id']
            test = Test(self.sink, data)
            if client_id in self.client_tests:
                log.info('Stopping previous test')
                self.client_tests[client_id].stop()

            self.client_tests[data['client_id']] = test
            test.run()
        else:
            log.warn('No test given')

    @action
    def stop_test(self, data):
        client_id = data['client_id']
        if client_id in self.client_tests:
            log.info('slave({}).stop_test client({})'.format(self.id, client_id))
            self.client_tests[client_id].stop()
            del self.client_tests[client_id]
        else:
            self.master.test_stopped()


class Test:
    running = False

    def __init__(self, sink, data):
        self.sink = sink

        test = data['test']
        self.runs = data['runs']
        self.name = test['name']
        self.base = test['base']
        self.actions = test['actions']
        self.client_id = data['client_id']

    def run(self):
        self.stop()
        self.running = True
        gevent.spawn(self.run_async)

    def stop(self):
        if self.running:
            self.running = False

    def run_async(self):
        log.info('TEST::run_async client({})'.format(self.client_id))

        self.sink.test_started({'client_id': self.client_id})
        for test_num in xrange(0, self.runs):
            if not self.running:
                log.info('TEST::run_async Detected kill')
                break
            self.sink.test_result(self.single_run())
        self.sink.test_finished({'client_id': self.client_id})

        log.info('TEST::run_async finished client({})'.format(self.client_id))

    def single_run(self):
        result = {
            'client_id': self.client_id,
            'actions': []
        }
        context = {
            '__builtins__': None,
            'rand': random.random
        }
        session = Session()
        for action in self.actions:
            if 'name' not in action or 'url' not in action:
                log.info('Error, incomplete action')
                return self.error('Got incomplete action')

            if self.parse_probability(action):
                if self.eval_condition(context, action):
                    run = self.test_action(context, session, action)
                    if run:
                        result['actions'].append(run)

        return result

    def error(self, error_str):
        self.sink.test_result({
            'client_id': self.client_id,
            'error': error_str
        })

    def parse_probability(self, action):
        if 'prob' in action:
            prob = self.eval_expressions(context, action['prob'])
            # Strip the % off
            try:
                if prob[-1:] == '%':
                    prob = float(prob[:-1]) / 100
                else:
                    prob = float(prob)
            except ValueError:
                self.error('Got non-percent or non-decimal probability in action {}'.format(action['name']))
                return False

            if prob <= random.random():
                return False
        return True

    def eval_expressions(self, context, input):
        return re.sub(r'\[\[.*?\]\]',
                      lambda match: str(self.eval(match.group()[2:-2], context)),
                      input)

    def eval_condition(self, context, action):
        if 'condition' in action:
            condition = self.eval_expressions(context, action['condition'])
            return self.eval(condition, context)
        return True

    def eval(self, in_str, context):
        try:
            return eval(in_str, context, {})
        except Exception as e:
            log.info('Error evaluating condition: {}'.format(e))
            self.error('Slave({}) action: {} got error evaluating condition: {}'.format(self.id, action['name'], e))

    def test_action(self, context, session, action):
        if 'name' in action:
            run = {'name': action['name']}
        else:
            log.warn('Missing name in action')
            return

        if 'url' in action:
            url = self.base + self.eval_expressions(context, action['url'])
        else:
            log.warn('Missing url in action')
            return

        if 'input' in action:
            input = self.eval_expressions(context, action['input'])
            async_request = grequests.post(url, input, session=session)
        else:
            async_request = grequests.get(url, session=session)
        e = gevent.spawn(async_request.send)

        start_time = time()
        e.join()
        response = e.value
        run['time_taken'] = time() - start_time

        if 'store_output' in action:
            try:
                context[action['store_output']] = response.json()
            except ValueError:
                context[action['store_output']] = response.text

        return run

@action_class
class Sink(Actionable):
    sock = None
    is_host = None
    connection_prefix = 'tcp://'
    listen_loop = None
    socket_lock = BoundedSemaphore(1)
    result_lock = BoundedSemaphore(1)
    test_results = {}
    RUNS_PER_SEND = 10

    def __init__(self, id, host, slave):
        self.id = id
        self.host = host
        self.slave = slave
        self.master = Sender(self.send_to_master)

    def send_to_master(self, data):
        data['sink_id'] = self.id
        self.slave.send_to_master(data)

    def send_to_sink(self, data):
        gevent.sleep(0)
        data['sink_id'] = self.id
        log.debug('{}->SINK'.format(data['action']))
        if not self.is_host:
            # If we're not the host send it
            self.sock.send(bson.dumps(data))
        else:
            # We're the host, so just handle it
            gevent.spawn(self.handle_sink_message, data)
        log.debug('{}->SINK end'.format(data['action']))

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
                    log.info('Sink({}) failed to {} to {}'.format(
                        self.id,
                        'bind' if self.is_host else 'connect',
                        self.host))

                log.info('Attempt {} of 3 failed...'.format(attempt))
                gevent.sleep(1)
        log.warn('Failed to connect')
        return False

    def create_socket(self, type):
        with Sink.socket_lock:
            self.close()
            self.sock = self.slave.context.socket(type)

    def close(self):
        if self.sock:
            self.sock.close()

    def listen_to_slaves(self):
        log.info('Sink listening')
        while True:
            try:
                self.handle_sink_message(bson.loads(self.sock.recv()))
            except ZMQError, e:
                log.info('Sink stopped listening, error: {}'.format(e))
                break

    def handle_sink_message(self, data):
        if 'action' in data:
            log.debug('{}<-SINK'.format(data['action']))
            self.run_action(data)
            log.debug('{}<-SINK end'.format(data['action']))
        else:
            log.warn('Slave sent message to sink with no action')

    @action
    def test_started(self, data):
        client_id = data['client_id']
        log.info('SINK({})::slave({}) test({})_started'.format(self.id, data['slave_id'], data['client_id']))
        if client_id not in self.test_results:
            log.info('First run, creating test({})'.format(client_id))
            self.test_results[client_id] = {
                'running_slaves': [],
                'participating_slaves': [],
                'pending': []
            }
            self.master.test_started(data)
        self.test_results[client_id]['running_slaves'].append(data['slave_id'])

    @action
    def test_result(self, data):
        client_id = data['client_id']
        slave_id = data['slave_id']
        log.info('SINK({})::slave({}) test({})_result'.format(self.id, slave_id, client_id))
        if client_id in self.test_results:
            self.test_results[client_id]['last_result'] = int(time())
            pending = self.test_results[client_id]['pending']
            pending.append(data['actions'])
            if slave_id not in self.test_results[client_id]['participating_slaves']:
                self.test_results[client_id]['participating_slaves'].append(slave_id)

            if len(pending) >= self.RUNS_PER_SEND:
                self.send_pending(client_id)
                log.info('SINK({})::Sent results, running: {}'.format(self.id, self.test_results[client_id]['running_slaves']))

        else:
            log.warn('Failed test not found'.format(client_id))

    def send_pending(self, client_id):
        if client_id in self.test_results:
            compiled_results = self.summarize_pending(client_id)
            if compiled_results:
                self.master.test_result(compiled_results)
            self.test_results[client_id]['pending'] = []
            self.test_results[client_id]['participating_slaves'] = []
        else:
            log.warn('Error sending pending, test not found.')

    def summarize_pending(self, client_id):
        results = self.test_results[client_id]
        pending = results['pending']
        if len(pending) == 0:
            return False

        summary = {
            'client_id': client_id,
            'slaves': results['participating_slaves'],
            'total_runs': 0,
            'actions': []
        }
        for run in pending:
            summary['total_runs'] += 1
            for i, action in enumerate(run):
                action_name = action['name']
                try:
                    action_summary = summary['actions'][i]
                except IndexError:
                    summary['actions'].append({
                        'name': action_name,
                        'runs': 0,
                        'avg_time': 0
                    })
                    action_summary = summary['actions'][i]

                runs = float(action_summary['runs'])
                if runs == 0:
                    action_summary['avg_time'] = action['time_taken']
                else:
                    action_summary['avg_time'] = (runs/(runs+1))*action_summary['avg_time'] + action['time_taken']/(runs+1)
                # print('Run: {}, took: {}, avg: {}'.format(runs, action['time_taken'], action_summary['avg_time']))
                action_summary['runs'] += 1

        return summary

    @action
    def test_finished(self, data):
        client_id = data['client_id']
        slave_id = data['slave_id']
        log.info('SINK({})::slave({}) test({})_finished'.format(self.id, slave_id, client_id))
        if client_id in self.test_results:
            running_slaves = self.test_results[client_id]['running_slaves']
            if slave_id in running_slaves:
                running_slaves.remove(slave_id)
            if len(running_slaves) == 0:
                self.send_pending(client_id)
                del self.test_results[client_id]
                self.master.test_finished({'client_id': client_id})
                print('Deleted test')
        else:
            log.warn('Failed, test results not found')


if __name__ == "__main__":
    print('Slave started')
    context = zmq.Context()
    slave = Slave(context)
    try:
        gevent.spawn(slave.heartbeat_forever)
        slave.listen_to_master()
    except KeyboardInterrupt:
        log.info('Telling master we quit')
        slave.master.quit({'id': slave.id})

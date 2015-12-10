from py.shared import Actionable, Sender, action, action_class
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
    """
    This class manages connections to the master and handles client-server logistics.
    Actions on it are run by master.
    """
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
        # This socket sends messages to master
        self.socket_out = context.socket(zmq.PUSH)
        self.socket_out.connect("tcp://localhost:{}".format(SLAVE_MASTER_PORT))
        # Syntax allowing self.master.<action>(data)
        self.master = Sender(self.send_to_master)

        # This socket receives broadcasts from master
        self.socket_in = context.socket(zmq.SUB)
        self.socket_in.setsockopt(zmq.SUBSCRIBE, "")
        self.socket_in.connect("tcp://localhost:{}".format(SLAVE_PUB_PORT))

        self.sink_host = '{}:{}'.format(self.hostname(), SLAVE_SINK_PORT)
        self.uuid = uuid4().hex

    def send_to_master(self, data):
        """
        Sends a bson serialized message to master, inserting slave specific information such as id
        :param data: Message data to send
            action: (required) RPC to call on serverside slave instance
        """
        if self.id:
            data['slave_id'] = self.id
        if data['action'] != 'heartbeat':
            log.debug('MASTER->{}'.format(data['action']))
        self.socket_out.send(bson.dumps(data))
        if data['action'] != 'heartbeat':
            log.debug('MASTER->{} end'.format(data['action']))

    def send_to_sink(self, data):
        """
        Send bson encoded data to the sink for collation
        :param data: Message data to send
        """
        if self.id:
            data['slave_id'] = self.id
        if self.sink_obj:
            self.sink_obj.send_to_sink(data)
        else:
            log.warn('{}->SINK failed, no sink'.format(data['action']))

    def listen_to_master(self):
        """
        Wait for messages from the server and delegate them
        """
        log.info("Listening to master")
        while True:
            self.handle_master_message(self.socket_in.recv())

    def handle_master_message(self, msg):
        """
        Decode a message from the server, if it matches our id or has no id decode and run it.
        :param msg: BSON encoded message from the server
            action: Local function to run
            slave_id: If present and not our id ignore message
        """
        data = bson.loads(msg)
        if 'action' in data:
            if 'slave_id' not in data or data['slave_id'] == self.id:
                log.debug('{}<-MASTER'.format(data['action']))
                self.run_action(data)
                log.debug('{}<-MASTER end'.format(data['action']))
        else:
            log.warn('Server sent message with no action')

    def heartbeat(self):
        """
        Generate a status report and send it to master
        """
        # physical_memory = psutil.phymem_usage()
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
                # 'physical': (physical_memory.used, physical_memory.total),
                'physical': (0, 0),
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
        """
        Set id if it matches our uuid
        :param data: Message specific data:
            slave_uuid: UUID to check against ours
            new_slave_id: Id to set if match
        """
        if 'slave_uuid' not in data or 'new_slave_id' not in data:
            log.warn('receiving id without slave_uuid/new_slave_id')
            return
        if data['slave_uuid'] == self.uuid:
            self.id = data['new_slave_id']
            log.info('Received id: {}'.format(self.id))

    @action
    def set_sink(self, data):
        """
        Set sink
        :param data: Message specific data:
            sink_id: Sink id to connect to
            sink_host: Host to connect to
        :return:
        """
        if 'sink_id' in data and 'sink_host' in data and data['sink_host'] == self.sink_host:
            self.connect_to_sink(data['sink_id'], data['sink_host'])
        else:
            log.warn('Incomplete sink call')

    @staticmethod
    def hostname():
        return socket.gethostname()

    def connect_to_sink(self, sink_id, host):
        """
        Attempt to connect to the given sink, and create a local sink instance
        :param sink_id: ID to assign to the sink
        :param host: host to connect to
        """
        log.info('Attempting to connect to sink({}) - {}'.format(sink_id, host))

        if self.sink_obj:
            log.info('Closing previous sink')
            self.sink_obj.close()

        # Create our sink
        self.sink_obj = Sink(id=sink_id,
                             host=host,
                             slave=self)

        # Initialize it, and tell master if successful
        if self.sink_obj.setup():
            log.info('Ok!')
            # RPC for sink
            self.sink = Sender(self.send_to_sink)
            self.master.connected_to_sink({
                'sink_id': self.sink_obj.id
            })

    @action
    def quit(self, data):
        """
        If this RPC is run master told this slave to quit
        """
        log.info('Master told us to quit, quitting.')
        sys.exit(0)

    @action
    def run_test(self, data):
        """
        Receive a test, instantiate it and run
        :param data: Message specific data:
            client_id: Client that started the test
            test: Test data to hand off to the test
        :return:
        """
        log.info('slave({}).run_test client({})'.format(self.id, data['client_id']))

        if not self.sink_obj:
            log.warn('Error, no sink')
            return

        if 'test' in data:
            client_id = data['client_id']
            test = Test(self.sink, data)

            # Only allow one test at a time
            if client_id in self.client_tests:
                log.info('Stopping previous test')
                self.client_tests[client_id].stop()

            self.client_tests[data['client_id']] = test
            test.run()
        else:
            log.warn('No test given')

    @action
    def stop_test(self, data):
        """
        Stop any tests from that client
        :param data: Message specific data:
            client_id: Client to stop tests for
        """
        client_id = data['client_id']
        if client_id in self.client_tests:
            log.info('slave({}).stop_test client({})'.format(self.id, client_id))

            self.client_tests[client_id].stop()
            del self.client_tests[client_id]
        else:
            self.master.test_stopped()


class Test:
    """
    This class is in charge of parsing and running tests, and reporting results to the sink
    Test data format:

    runs: # Of times to run the test
    client_id: Client that started the test
    test:
        name: Name of test
        base: Base URL
        actions: List of urls to hit
            [
                name: Name of the action
                url: relative url to access
    """
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
        """
        Start the test in a greenlet, stopping first if already running
        """
        self.stop()
        self.running = True
        gevent.spawn(self.run_async)

    def stop(self):
        """
        Top the test if it is running
        """
        if self.running:
            self.running = False

    def run_async(self):
        """
        Start the test running, repeating all actions runs times, reporting to the sink after every run
        :return:
        """
        log.info('TEST::run_async client({})'.format(self.client_id))

        # Inform the sink that this test is started
        self.sink.test_started({'client_id': self.client_id})

        for test_num in xrange(0, self.runs):
            # Detect if the test has been stopped.
            if not self.running:
                log.info('TEST::run_async Detected kill')
                break
            # Run a single iteration, sending results to the sink
            self.sink.test_result(self.single_run())

        # Inform the sink that this test is finished
        self.sink.test_finished({'client_id': self.client_id})

        log.info('TEST::run_async finished client({})'.format(self.client_id))

    def single_run(self):
        result = {
            'client_id': self.client_id,
            'actions': []
        }

        # Globals for the eval method
        context = {
            '__builtins__': None,
            'rand': random.random
        }

        # All requests in a test happen in one session
        session = Session()
        for action in self.actions:
            if 'name' not in action or 'url' not in action:
                log.info('Error, incomplete action')
                return self.error('Got incomplete action')

            # Conditionally hit url:
            if self.parse_probability(action):
                # Evaluate condition:
                if self.eval_condition(context, action):
                    # Actually access the url
                    run = self.test_action(context, session, action)
                    if run:
                        # Append results
                        result['actions'].append(run)

        return result

    def error(self, error_str):
        self.sink.test_result({
            'client_id': self.client_id,
            'error': error_str
        })

    def parse_probability(self, action):
        """
        Some actions have a random chance of occurring
        :param action: A test action
            prob: (optional) if present then only occur this fraction of times, ex: 10% or .1
        :return:
        """
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

            if prob > random.random():
                return False
        return True

    def eval_expressions(self, context, input):
        """
        URLs/data can contain expressions, this evaluates them as python
        :param context: Execution context, "globals"
        :param input: A string input to search for expressions
        :return: The input with all expressions replaces with their results
        """
        return re.sub(r'\[\[.*?\]\]',
                      lambda match: str(self.eval(match.group()[2:-2], context)),
                      input)

    def eval_condition(self, context, action):
        """
        Actions can have a condition, which is evaluated and only run if true
        :param context: Execution context
        :param action: Action being run:
            condition: (optional)
        :return: Whether the found condition evaluates to true
        """
        if 'condition' in action:
            condition = self.eval_expressions(context, action['condition'])
            return self.eval(condition, context)
        return True

    def eval(self, in_str, context):
        """
        Evaluate the python
        :param in_str: Code to run
        :param context: Execution context
        :return: Result of the expression
        """
        try:
            return eval(in_str, context, {})
        except Exception as e:
            log.info('Error evaluating condition: {}'.format(e))
            self.error('Slave({}) action: {} got error evaluating condition: {}'.format(self.id, action['name'], e))

    def test_action(self, context, session, action):
        """
        Run a single action on the test, which usually means hit a url
        :param context: Execution context
        :param session: Current test run session
        :param action: Action to run
            name: Name of the test action
            url: URL to hit
            input: Data to send, if present request is a POST
            store_output: Variable to store the response from the url
        :return: A test hit result:
            name: Name of action run
            time_taken: Time taken for request to complete
        """
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
    """
    This class is responsible for aggregating test results from slaves and sending it to the master.
    It is a bad case of premature optimization.

    TODO: Remove this class and everything it stands for. Create a new thing separate from slaves.
    """
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
        # Sender syntax
        self.master = Sender(self.send_to_master)

    def send_to_master(self, data):
        """
        Send a message to master, runs on the slave RPC
        :param data:
        :return:
        """
        data['sink_id'] = self.id
        self.slave.send_to_master(data)

    def send_to_sink(self, data):
        """
        Since slaves are sinks, if the slave is trying to send to its self then just handle it
        :param data:
        :return:
        """
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
        """
        Attempt to set up sink connection
        :return: Boolean success
        """
        self.is_host = self.id == self.slave.id
        self.create_socket(zmq.PULL if self.is_host else zmq.PUSH)

        for attempt in xrange(1, 4):
            # This frequently fails for a currently unknown reason, so try it a couple times
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
        """
        Listen for messages coming in from slaves
        :return:
        """
        log.info('Sink listening')
        while True:
            try:
                self.handle_sink_message(bson.loads(self.sock.recv()))
            except ZMQError, e:
                log.info('Sink stopped listening, error: {}'.format(e))
                break

    def handle_sink_message(self, data):
        """
        Handle incoming RPC messages
        :param data:
        """
        if 'action' in data:
            log.debug('{}<-SINK'.format(data['action']))
            self.run_action(data)
            log.debug('{}<-SINK end'.format(data['action']))
        else:
            log.warn('Slave sent message to sink with no action')

    @action
    def test_started(self, data):
        """
        Tell master that a slave has started a test
        :param data: Message specific data:
            client_id: Client that started the test
            slave_id: Slave running the test
        """
        client_id = data['client_id']
        log.info('SINK({})::slave({}) test({})_started'.format(self.id, data['slave_id'], data['client_id']))

        # Register the result store
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
        """
        Receive a test result from a slave. Store results in pending until we have enough runs to aggregate and send
        :param data:
        """
        client_id = data['client_id']
        slave_id = data['slave_id']
        log.info('SINK({})::slave({}) test({})_result'.format(self.id, slave_id, client_id))

        if client_id in self.test_results:
            self.test_results[client_id]['last_result'] = int(time())

            pending = self.test_results[client_id]['pending']
            pending.append(data['actions'])

            if slave_id not in self.test_results[client_id]['participating_slaves']:
                self.test_results[client_id]['participating_slaves'].append(slave_id)

            # Only send sometimes
            if len(pending) >= self.RUNS_PER_SEND:
                self.send_pending(client_id)
                log.info('SINK({})::Sent results, running: {}'.format(self.id, self.test_results[client_id]['running_slaves']))

        else:
            log.warn('Failed test not found'.format(client_id))

    def send_pending(self, client_id):
        """
        Send pending results to the client through the master
        :param client_id:
        """
        if client_id in self.test_results:
            compiled_results = self.summarize_pending(client_id)
            if compiled_results:
                self.master.test_result(compiled_results)
            self.test_results[client_id]['pending'] = []
            self.test_results[client_id]['participating_slaves'] = []
        else:
            log.warn('Error sending pending, test not found.')

    def summarize_pending(self, client_id):
        """
        Summarize the runs before sending results to the master
        :param client_id:
        :return:
        """
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
                    # This computes a total average without storing historical data.
                    action_summary['avg_time'] = (runs/(runs+1))*action_summary['avg_time'] + action['time_taken']/(runs+1)
                # print('Run: {}, took: {}, avg: {}'.format(runs, action['time_taken'], action_summary['avg_time']))
                action_summary['runs'] += 1

        return summary

    @action
    def test_finished(self, data):
        """
        Tell that the test has finished and send any remaining data
        :param data:
            client_id: Client that started the test
            slave_id: Slave that finished
        :return:
        """
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

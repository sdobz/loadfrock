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
from py.action_wrapper import ActionWrapper,\
    WebSocketBroadcastActionWrapper,\
    WebSocketReplyActionWrapper,\
    SlaveBroadcastActionWrapper


# http://css.dzone.com/articles/gevent-zeromq-websockets-and
class MasterApplication(Actionable):
    handler = None
    slave_registry = {}
    next_slave_id = 0
    next_websocket_id = 0
    testing = False
    websockets = ActionWrapper(None)  # Defaults to do nothing
    test = {'actions':[]}

    def __init__(self, context):
        print('Master initialized')
        self.context = context
        self.slave_in = context.socket(zmq.REP)
        self.slave_in.bind("tcp://*:{}".format(SLAVE_REP_PORT))
        gevent.spawn(self.listen_to_slaves)

        self.slave_out = context.socket(zmq.PUB)
        self.slave_out.bind("tcp://*:{}".format(SLAVE_PUB_PORT))

        self.slaves = SlaveBroadcastActionWrapper(self.slave_out)

        self.slave_actions = SlaveActions(self)
        self.websocket_actions = WebSocketActions(self)

    def __call__(self, environ, start_response):
        ws = environ['wsgi.websocket']
        self.handler = ws.handler
        self.listen_to_websocket(ws)

    def listen_to_websocket(self, ws):
        print('Listening to websockets')
        self.websockets = WebSocketBroadcastActionWrapper(ws.handler)

        while True:
            try:
                self.handle_websocket_message(ws, ws.receive())
            except WebSocketError:
                break

    def handle_websocket_message(self, ws, msg):
        if msg:
            data = json.loads(msg)
            if 'action' in data:
                reply = WebSocketReplyActionWrapper(ws)
                self.websocket_actions.run_action(data, reply)

    def get_websocket_id(self):
        self.next_websocket_id += 1
        return self.next_websocket_id - 1

    def listen_to_slaves(self):
        print('Listening to slaves')
        while True:
            msg = self.slave_in.recv()
            resp = self.handle_slave_message(msg)
            self.slave_in.send(resp)

    def handle_slave_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            resp = self.slave_actions.run_action(data) or {}
        else:
            resp = {'error': 'No action specified'}
        return bson.dumps(resp)

    def register_slave(self, slave):
        slave.id = self.next_slave_id
        self.slave_registry[slave.id] = slave
        self.next_slave_id += 1

    def get_slave(self, id):
        if not id in self.slave_registry:
            print('Slave id: {} not found, adding.'.format(id))
            self.slave_registry[id] = Slave(self, id)
        return self.slave_registry[id]

    def remove_slave(self, id):
        if id in self.slave_registry:
            print('Killing slave {}'.format(id))
            del self.slave_registry[id]
        self.slaves.quit({'id': id})
        self.websockets.slave_disconnected({'id': id})

    def check_slaves(self):
        the_time = int(time())
        # Since we modify the dict (and that messes with iteration) first get a static list of all of them
        slave_ids = self.slave_registry.keys()
        for slave_id in slave_ids:
            slave = self.slave_registry[slave_id]
            if slave.last_beat and\
               the_time - slave.last_beat[0] > HEARTBEAT_PERIOD * BEATS_TO_KILL and\
               slave.last_beat[0] != 0:
                self.remove_slave(slave_id)

    def watch_slaves(self):
        while True:
            self.check_slaves()
            gevent.sleep(HEARTBEAT_PERIOD)


class Slave:
    def __init__(self, master, id=None):
        self.master = master
        self.last_beat = (0, {})
        self.id = id

    def log_heartbeat(self, data):
        the_time = int(time())
        self.last_beat = (the_time, data)


class MasterActions(Actionable):
    websockets = property(lambda self: self.master.websockets)
    slaves = property(lambda self: self.master.slaves)

    def __init__(self, master):
        self.master = master

@action_class
class SlaveActions(MasterActions):
    @action
    def connect(self, data):
        slave = Slave(self.master)
        self.master.register_slave(slave)
        print("Slave {} connected!".format(slave.id))
        self.websockets.slave_connected({'id': slave.id})
        return {'id': slave.id}

    @action
    def heartbeat(self, data):
        if not 'id' in data:
            return {'error': 'id not specified'}

        slave = self.master.get_slave(data['id'])
        slave.log_heartbeat(data)
        self.websockets.slave_heartbeat(data)

    @action
    def quit(self, data):
        if not 'id' in data:
            return {'error': 'id not specified'}

        self.master.remove_slave(data['id'])

@action_class
class WebSocketActions(MasterActions):
    @action
    def connect(self, data, reply):
        id = self.master.get_websocket_id()
        print('Websocket {} connected!'.format(id))
        reply.set_id({'id': id})

    @action
    def quit(self, data, reply):
        self.master.remove_slave(data['id'])
        self.slaves.quit(data)

    @action
    def request_slaves(self, data, reply):
        data = {'slaves': dict((slave.id, slave.last_beat[1]) for slave in self.master.slave_registry.values())}
        reply.receive_slaves(data)

    @action
    def request_test(self, data, reply):
        reply.receive_test({'test': self.master.test})

    @action
    def set_test_prop(self, data, reply):
        print(data)
        self.master.test[data['prop']] = data['value']
        self.websockets.set_test_prop(data)

    @action
    def request_available_tests(self, data, reply):
        tests_glob = os.path.join(os.path.dirname(__file__), TEST_DIR, '*.json')
        files = glob.glob(tests_glob)
        files_stripped = [os.path.basename(filename)[:-5] for filename in files]
        reply.receive_available_tests({'tests': files_stripped})

    @action
    def load_test(self, data, reply):
        filename = os.path.join(os.path.dirname(__file__), TEST_DIR, data['name'] + '.json')
        if os.path.exists(filename):
            with open(filename) as file:
                self.master.test = json.loads(file.read())
        self.websockets.receive_test({'test': self.master.test})

    @action
    def save_test(self, data, reply):
        if 'name' in self.master.test:
            filename = os.path.join(os.path.dirname(__file__), TEST_DIR, self.master.test['name'] + '.json')
            with open(filename, 'w') as file:
                file.write(json.dumps(self.master.test))
            reply.save_successful()

    @action
    def add_action(self, data, reply):
        self.master.test['actions'].insert(data['index'], {})
        self.websockets.add_action(data)

    @action
    def set_action_prop(self, data, reply):
        actions = self.master.test['actions']
        if data['index'] in actions:
            actions[data['index']][data['prop']] = data['value']


if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()
    master_app = MasterApplication(context)
    gevent.spawn(master_app.watch_slaves)
    WebSocketServer(('', WEBSOCKET_PORT), master_app).start()
    gevent.wsgi.WSGIServer(
        ('', HTTP_PORT), paste.urlparser.StaticURLParser(os.path.dirname(__file__))).serve_forever()

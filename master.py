from py.shared import *
from config import *
import os
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
    testing = False
    websockets = ActionWrapper(None)  # Defaults to do nothing

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
        print('Killing slave {}'.format(id))
        del self.slave_registry[id]
        self.websockets.slave_disconnected({'id': id})

    def check_slaves(self):
        the_time = int(time())
        # Since we modify the dict (and that messes with iteration) first get a static list of all of them
        slave_ids = self.slave_registry.keys()
        for slave_id in slave_ids:
            slave = self.slave_registry[slave_id]
            if slave.last_beat and the_time - slave.last_beat[0] > HEARTBEAT_PERIOD * BEATS_TO_KILL:
                slave.kill()

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

    def kill(self):
        # Ask it to quit gracefully, can't hurt
        self.master.remove_slave(self.id)
        self.master.slaves.quit({'targets': [self.id]})


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
        print("Slave connected!")
        self.websockets.slave_connected({'id': slave.id})
        return {'id': slave.id}

    @action
    def heartbeat(self, data):
        if not 'id' in data:
            return {'error': 'id not specified'}

        slave = self.master.get_slave(data['id'])
        slave.log_heartbeat(data)
        self.websockets.slave_heartbeat(data)


@action_class
class WebSocketActions(MasterActions):
    @action
    def connect(self, data, reply):
        print('Websocket connected!')

    @action
    def quit(self, data, reply):
        self.master.remove_slave(data['id'])
        self.slaves.quit(data)

    @action
    def request_slaves(self, data, reply):
        data = {'slaves': dict((slave.id, slave.last_beat[1]) for slave in self.master.slave_registry.values())}
        reply.receive_slaves(data)


if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()
    master_app = MasterApplication(context)
    gevent.spawn(master_app.watch_slaves)
    WebSocketServer(('', WEBSOCKET_PORT), master_app).start()
    gevent.wsgi.WSGIServer(
        ('', HTTP_PORT), paste.urlparser.StaticURLParser(os.path.dirname(__file__))).serve_forever()

from shared import *
import os
import bson
import json
import paste.urlparser
import gevent
import gevent.wsgi
import zmq.green as zmq
from geventwebsocket import WebSocketServer, WebSocketError


class BroadcastActionWrapper:
    def __init__(self, socket):
        self.socket = socket

    def __getattr__(self, item):
        def go_for_it(data):
            data['action'] = item
            self.socket.send_string(bson.dumps(data))
        return go_for_it


class WebSocketsActionWrapper:
    def __init__(self, master):
        self.master = master

    def __getattr__(self, item):
        def go_for_it(data):
            data['action'] = item
            self.master.ws_broadcast(data)
        return go_for_it


# http://css.dzone.com/articles/gevent-zeromq-websockets-and
class MasterApplication(Actionable):
    handler = None

    def __init__(self, context):
        print('Master initialized')
        self.context = context
        self.slave_in = context.socket(zmq.REP)
        self.slave_in.bind("tcp://*:5566")
        gevent.spawn(self.listen_to_slaves)

        self.slave_out = context.socket(zmq.PUB)
        self.slave_out.bind("tcp://*:5588")

        self.slaves = BroadcastActionWrapper(self.slave_out)
        self.websockets = WebSocketsActionWrapper(self)

        self.slave_actions = SlaveActions(self)
        self.websocket_actions = WebSocketActions(self)

    def __call__(self, environ, start_response):
        ws = environ['wsgi.websocket']
        self.handler = ws.handler
        self.listen_to_websocket(ws)

    def listen_to_websocket(self, ws):
        print('Listening to websockets')
        while True:
            try:
                self.handle_websocket_message(ws, ws.receive())
            except WebSocketError:
                break

    def handle_websocket_message(self, ws, msg):
        if msg:
            data = json.loads(msg)
            if 'action' in data:
                self.websocket_actions.run_action(data['action'], ws, data)

    def ws_reply(self, ws, data):
        ws.send(json.dumps(data))

    def ws_broadcast(self, data):
        if self.handler:
            for client in self.handler.server.clients.values():
                client.ws.send(json.dumps(data))

    def listen_to_slaves(self):
        print('Listening to slaves')
        while True:
            self.slave_in.send(
                self.handle_slave_message(
                    self.slave_in.recv()))

    def handle_slave_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            resp = self.slave_actions.run_action(data['action'], data)
        else:
            resp = {'error': 'No action specified'}
        return bson.dumps(resp)

    def sl_broadcast(self, data):
        self.slave_out.send(bson.dumps(data))


class MasterActions(Actionable):
    def __init__(self, master):
        self.master = master
        self.slaves = master.slaves
        self.websockets = master.websockets

@action_class
class SlaveActions(MasterActions):
    @action
    def connect(self, data):
        print("Slave connected!")
        self.master.websockets.echo({'slave connected': 0})
        return {'id': 0}

@action_class
class WebSocketActions(MasterActions):
    def reply(self, ws, data):
        self.master.ws_reply(ws, data)

    @action
    def connect(self, ws, data):
        print('Websocket connected!')

    @action
    def echo(self, ws, data):
        print('Websocket told me to echo: {}'.format(data))
        self.slaves.echo(data)
        self.websockets.echo(data)
        self.reply(ws, {'type': 'reply echo', 'data': data})

if __name__ == "__main__":
    print('WLOCOM TO TEH LODE OF ETST')
    context = zmq.Context()

    WebSocketServer(('', 5577), MasterApplication(context)).start()
    gevent.wsgi.WSGIServer(
        ('', 5500), paste.urlparser.StaticURLParser(os.path.dirname(__file__))).serve_forever()

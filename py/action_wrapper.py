import bson
import json

# This is a bit of an RPC system, on the sending end
# Whenever you do actionwrapper.attribute(data)
# It sets data['action'] to attribute, then calls run
# The init stores one value (item), which is usually a socket of some sort


class ActionWrapper:
    def __init__(self, item):
        self.item = item

    def __str__(self):
        return 'Actionwrapper around: {}'.format(self.item)

    def __getattr__(self, action):
        def go_for_it(data=None):
            if not data:
                data = {}
            data['action'] = action
            return self.run(data)
        return go_for_it

    def run(self, data):
        pass


# Slave to master request/response
class MasterActionWrapper(ActionWrapper):
    def run(self, data):
        self.item.send(bson.dumps(data))
        return bson.loads(self.item.recv())


# Master to slaves broadcast
class SlaveBroadcastActionWrapper(ActionWrapper):
    def run(self, data):
        self.item.send(bson.dumps(data))


# Master to websockets broadcast
class WebSocketBroadcastActionWrapper(ActionWrapper):
    def run(self, data):
        for client in self.item.server.clients.values():
            client.ws.send(json.dumps(data))


# Master to specific websocket reply, only available after receiving a message
class WebSocketReplyActionWrapper(ActionWrapper):
    def __init__(self, websocket, id):
        self.item = websocket
        self.id = id

    def run(self, data):
        self.item.send(json.dumps(data))

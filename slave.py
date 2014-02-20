from shared import *
import zmq.green as zmq
import bson


class ReplyActionWrapper:
    def __init__(self, socket):
        self.socket = socket

    def __getattr__(self, item):
        def go_for_it(data={}):
            data['action'] = item
            self.socket.send(bson.dumps(data))
            return bson.loads(self.socket.recv())
        return go_for_it


@action_class
class Slave(Actionable):
    def __init__(self, context):
        self.socket_out = context.socket(zmq.REQ)
        self.socket_out.connect("tcp://localhost:5566")
        self.master = ReplyActionWrapper(self.socket_out)

        self.socket_in = context.socket(zmq.SUB)
        self.socket_in.setsockopt(zmq.SUBSCRIBE, "")
        self.socket_in.connect("tcp://0.0.0.0:5588")

    def listen_to_master(self):
        print("Listening to master")
        while True:
            self.handle_master_message(self.socket_in.recv_string())

    def handle_master_message(self, msg):
        data = bson.loads(msg)
        if 'action' in data:
            self.run_action(data['action'], data)
        else:
            print('Server sent message with no action')

    @action
    def echo(self, data):
        print('Master told us to echo: {}'.format(data))


if __name__ == "__main__":
    context = zmq.Context()
    slave = Slave(context)
    print("Slave connecting, id: {}".format(slave.master.connect()['id']))
    slave.listen_to_master()

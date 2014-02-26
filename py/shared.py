# This is the receiving end of the rpc system
# A class is given the decorator @action_class
# And methods are given @action
# run_action(data) attempts to call the method specified by data['action'] with data as an argument
import gevent

def action_class(cls):
    for name, method in cls.__dict__.iteritems():
        if hasattr(method, "is_action"):
            cls.actions.append(method.__name__)
    return cls


def action(f):
    f.is_action = True
    return f


class ActionError(Exception):
    pass


class Actionable(object):
    actions = []

    def run_action(self, data, *args, **kwargs):
        if 'action' not in data or data['action'] not in self.actions:
            return {'error': 'Unknown action'}
        action_func = getattr(self, data['action'])
        greenlet = gevent.spawn(action_func, data, *args, **kwargs)
        greenlet.start()
        gevent.sleep(0)
        greenlet.join()
        return greenlet.value


class Sender:
    def __init__(self, send_method):
        self.send_method = send_method

    def __getattr__(self, action):
        if self.send_method:
            def go_for_it(data=None):
                if not data:
                    data = {}
                data['action'] = action
                gevent.spawn(self.send_method, data).start()
            return go_for_it

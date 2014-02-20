import bson

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

    def run_action(self, action, *args, **kwargs):
        if action not in self.actions:
            return {'error': 'Unknown action'}
        return getattr(self, action)(*args, **kwargs)

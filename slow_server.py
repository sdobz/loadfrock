# A very simple HTTP server designed to for testing situations where the data returned
# is not important but the rate at which it comes down is. This server can be started
# using the command: python slow_server.py
#
# Once started, it will listen for requests on port 8000
# Requests should be of the form http://<address>:8000/size=<bytes>,duration=<seconds>
# where: <bytes> is the size of the response data
# and    <seconds> is how long you want it to take (at minimum, it may take longer)
#
# Notes:
# * The timing is pretty inaccurate for small byte sizes, this isn't a problem for
#   what I need it for
# * Press ctrl-c to stop serving


from gevent import wsgi
import gevent
import math
from time import time

MIN_SLEEP = .05


def slow_response(env, start_response):
    print(env['PATH_INFO'])
    request = env['PATH_INFO'].strip('/')
    duration = 1
    size = 1024

    valid_request = False
    params = request.split(",")
    for p in params:
        temp = p.partition("=")
        if temp[0] == "size":
            size = int(temp[2])
            valid_request = True
        elif temp[0] == "duration":
            duration = float(temp[2])
            valid_request = True

        if not valid_request:
            start_response('404 Not Found', [('Content-Type', 'text/html')])
            return ["Not Understood"]
    start_response('200 OK', [('Content-Type', 'text/html'),
                              ('Pragma', 'no-cache'),
                              ('Content-Length', str(size))])
    return slow_write(size, duration)


def slow_write(size, duration):
    bytes_written = 0
    start_time = time()
    duration_per_byte = duration/size

    chunk_size = int(math.ceil(MIN_SLEEP / duration_per_byte))
    sleep_duration = duration_per_byte * chunk_size

    while bytes_written < size:
        num_bytes = min(size - bytes_written, chunk_size)

        yield '.' * num_bytes
        bytes_written += num_bytes

        gevent.sleep(sleep_duration)
    now = time()
    print("Request took {} seconds".format(now - start_time))

if __name__ == '__main__':
    print "Listening on 0.0.0.0:8000"
    wsgi.WSGIServer(('', 8000), slow_response).serve_forever()

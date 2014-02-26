# A very simple HTTP server designed to for testing situations where the data returned
# is not important but the rate at which it comes down is. This server can be started
# using the command: python delayserver.py
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


import time
import BaseHTTPServer
import math

MIN_SLEEP = .05

class MyHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        request = self.path.strip("/")
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
                self.send_error(404)
                return

        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.slow_write(self.wfile, size, duration)

    def slow_write(self, output, size, duration):
        bytes_written = 0
        start_time = time.time()
        duration_per_byte = duration/size

        chunk_size = int(math.ceil(MIN_SLEEP / duration_per_byte))
        sleep_duration = duration_per_byte * chunk_size

        while bytes_written < size:
            num_bytes = min(size - bytes_written, chunk_size)

            output.write('.' * num_bytes)
            bytes_written += num_bytes

            time.sleep(sleep_duration)
            output.flush()
        now = time.time()
        self.log_message("Request took %f seconds",   now - start_time)

if __name__ == "__main__":
    http = BaseHTTPServer.HTTPServer(('', 8000), MyHTTPRequestHandler)
    print "Listening on 8000 - press ctrl-c to stop"
    http.serve_forever()
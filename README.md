# loadfrock

<img src="https://github.com/sdobz/loadfrock/blob/master/img/LoadFrock.png?raw=true" />

Loadfrock is a distributed HTTP load tester spawned from my desire to learn about networking, distributed applications, and concurrent programming.

It has several components:

* Browser - UI
    * Sends commands/tests to master
    * Reports on system status
    * Builds tests
* Master - Primary server
    * Serves HTTP static files to browsers
    * Serves websocket to browsers
    * Broadcasts messages to slaves
    * Receives test results from sinks
    * Sends test results to browsers
* Slave - Test runner
    * Receives broadcasts from master
    * Manages load tests
    * Sends results to sink
* Sink - Test aggregator (is a promoted slave)
    * Receives results from slaves
    * Aggregats and summarizes results
    * Sends summary to master

Communication is done with a custom RPC interface. Messages are sent as serialized python dicts. The dict has an `action` parameter which specifies which method should be run on the receiving class.

Network traffic is sent using ZMQ between slaves and master, and websockets between master and the browser.
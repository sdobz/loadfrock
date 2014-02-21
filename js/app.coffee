app = angular.module 'LoadFrock', ['ui.bootstrap']

app.filter 'bytes', ->
	(bytes, precision) ->
		if isNaN(parseFloat(bytes)) || !isFinite(bytes)
      return '-'
		if typeof precision == 'undefined'
      precision = 1
		units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB']
		number = Math.floor Math.log(bytes) / Math.log(1024)
		return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +  ' ' + units[number]

WEBSOCKET_PORT = 5577

app.service 'MasterService', ($rootScope) ->
  ws = new WebSocket('ws://localhost:' + WEBSOCKET_PORT)
  MasterService =
    connected: false
    id: null
    connection_actions: []
    actions: {}
    register_action: (action, callback) ->
      @actions[action] = callback
    register_connection_action: (callback) ->
      if not @connected
        @connection_actions.push(callback)
      else
        callback()
    run_action: (action, data) ->
      if @connected
        if data == undefined
          data = {}
        data['action'] = action
        ws.send JSON.stringify data

  ws.onopen = ->
    $rootScope.$apply ->
      MasterService.connected = true
      MasterService.run_action 'connect'
      for connection_action in MasterService.connection_actions
        connection_action()

  ws.onclose = ->
    $rootScope.$apply ->
      MasterService.connected = false

  ws.onmessage = (msg) ->
    if msg
      data = JSON.parse msg.data
      if data['action']
        $rootScope.$apply ->
          MasterService.actions[data['action']](data)

  MasterService.register_action 'set_id', (data) ->
    console.log 'Given id: ', data['id']
    MasterService.id = data['id']

  return MasterService


app.controller 'FrockBodyCtrl', ($scope, MasterService) ->
  $scope.connection = MasterService

app.service 'SlaveService', (MasterService) ->
  SlaveService =
    slaves: []
    kill_slave: (id) ->
      MasterService.run_action 'quit',
        id: id

  MasterService.register_connection_action ->
    MasterService.run_action 'request_slaves'
  MasterService.register_action 'receive_slaves', (data) ->
    angular.extend SlaveService.slaves, data['slaves']

  MasterService.register_action 'slave_connected', (data) ->
    if not SlaveService.slaves[data['id']]
      SlaveService.slaves[data['id']] = {}
    angular.extend SlaveService.slaves[data['id']], data

  MasterService.register_action 'slave_disconnected', (data) ->
    if SlaveService.slaves[data['id']]
      delete SlaveService.slaves[data['id']]

  MasterService.register_action 'slave_heartbeat', (data) ->
    if not SlaveService.slaves[data['id']]
      SlaveService.slaves[data['id']] = {}
    angular.extend SlaveService.slaves[data['id']], data


  return SlaveService

app.controller 'SlaveCtrl', ($scope, SlaveService) ->
  $scope.slave_service = SlaveService

app.service 'Test', (MasterService) ->
  Test =
    name: ''
    runs: ''
    base: ''
    actions: []

    send_test_prop: (prop) ->
      console.log Test
      MasterService.run_action 'set_test_prop',
        source: MasterService.id
        prop: prop
        value: Test[prop]

    send_action_prop: (index, prop) ->
      MasterService.run_action 'set_action_prop',
        source: MasterService.id
        index: index
        prop: prop
        value: Test.actions[index][prop]

    add_action: (index) ->
      MasterService.run_action 'add_action',
        source: MasterService.id
        index: index

    save_alert_visible: false
    save: ->
      MasterService.run_action 'save_test'

  MasterService.register_connection_action ->
    MasterService.run_action 'request_test'
  MasterService.register_action 'receive_test', (data) ->
    angular.extend Test, data['test']

  MasterService.register_action 'set_test_prop', (data) ->
    if data['source'] != MasterService.id
      Test[data['prop']] = data['value']

  MasterService.register_action 'set_action_prop', (data) ->
    if data['source'] != MasterService.id
      Test.actions[data['index']][data['prop']] = data['value']

  MasterService.register_action 'add_action', (data) ->
    Test.actions.splice(data['index'], 0, {})

  MasterService.register_action 'save_successful', ->
    Test.save_alert_visible = true

  return Test

app.controller 'TestCtrl', ($scope, Test, $modal) ->
  $scope.test = Test

  $scope.send_test_prop = Test.send_test_prop
  $scope.send_action_prop = Test.send_action_prop

  $scope.open_load_modal = ->
    $modal.open
      templateUrl: 'load_modal.html'
      controller: LoadModalCtrl

app.service 'TestLoader', (MasterService) ->
  TestLoader =
    available_tests: []
    load_available: ->
      MasterService.run_action 'request_available_tests'

    load_test: (test_name) ->
      MasterService.run_action 'load_test',
        name: test_name

  MasterService.register_action 'receive_available_tests', (data) ->
    TestLoader.available_tests = data['tests']

  return TestLoader


LoadModalCtrl = ($scope, $modalInstance, TestLoader) ->
  $scope.test_loader = TestLoader
  TestLoader.load_available()

  $scope.load = (test_name) ->
    TestLoader.load_test test_name
    $modalInstance.close ''

  $scope.cancel = ->
    $modalInstance.dismiss ''


app.controller 'ActionCtrl', ($scope, ActionServer) ->

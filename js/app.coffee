app = angular.module 'LoadFrock', ['ui.bootstrap', 'dragger']

app.filter 'bytes', ->
	(bytes, precision) ->
		if isNaN(parseFloat(bytes)) || !isFinite(bytes)
      return '-'
		if typeof precision == 'undefined'
      precision = 1
		units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB']
		number = Math.floor Math.log(bytes) / Math.log(1024)
		return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +  ' ' + units[number]

app.directive 'autoGrow', ->
	(scope, element, attr) ->
    minHeight = element[0].offsetHeight
    paddingLeft = element.css('paddingLeft')
    paddingRight = element.css('paddingRight')

    $shadow = angular.element('<div></div>').css
      position: 'absolute',
      top: -10000
      left: -10000
      width: element[0].offsetWidth - parseInt(paddingLeft || 0) - parseInt(paddingRight || 0)
      fontSize: element.css('fontSize')
      fontFamily: element.css('fontFamily')
      lineHeight: element.css('lineHeight')
      resize:     'none'
    angular.element(document.body).append $shadow

    update = ->
      times = (string, number) ->
        Array(number+1).join string

      val = element.val().replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/&/g, '&amp;')
        .replace(/\n$/, '<br/>&nbsp;')
        .replace(/\n/g, '<br/>')
        .replace(/\s{2,}/g, (space) -> times('&nbsp;', space.length - 1) + ' ')
      $shadow.html(val)

      element.css('height', Math.max($shadow[0].offsetHeight + 10, minHeight) + 'px')

    element.bind('keyup keydown keypress change focus', update)
    update()


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

  MasterService.register_action 'error', (data) ->
    console.log 'Got error ', data['error']

  return MasterService


app.controller 'FrockBodyCtrl', ($scope, MasterService) ->
  $scope.connection = MasterService
  $scope.show_test = true
  $scope.show_results = false

app.service 'SlaveService', (MasterService) ->
  SlaveService =
    slaves: {}
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

    save_alert_visible: false
    save: ->
      MasterService.run_action 'save_test',
        test: Test

    add_action: (index) ->
      Test.actions.splice index, 0, {}

    delete_action: (index) ->
      Test.actions.splice index, 1

  MasterService.register_action 'receive_test', (data) ->
    angular.extend Test, data['test']

  MasterService.register_action 'save_successful', ->
    console.log 'Save ok'
    Test.save_alert_visible = true

  return Test

app.service 'TestLoader', (MasterService) ->
  TestLoader =
    available_tests: []
    load_available: ->
      MasterService.run_action 'request_available_tests'

    load_test: (test_name) ->
      MasterService.run_action 'request_test',
        name: test_name

  MasterService.register_action 'receive_available_tests', (data) ->
    TestLoader.available_tests = data['tests']

  return TestLoader

app.controller 'TestCtrl', ($scope, Test, TestLoader, TestRunner, ResultsService, $modal) ->
  $scope.test = Test
  $scope.test_runner = TestRunner

  $scope.open_load_modal = ->
    $modal.open
      templateUrl: 'load_modal.html'
      controller: TestLoaderCtrl
    .result.then (test_name) ->
      TestLoader.load_test test_name

  $scope.open_delete_action_modal = (index) ->
    scope = $scope.$new true
    scope.action = Test.actions[index]
    $modal.open
      templateUrl: 'action_delete.html'
      scope: scope
    .result.then ->
      Test.delete_action index


TestLoaderCtrl = ($scope, $modal, TestLoader, MasterService) ->
  $scope.test_loader = TestLoader
  TestLoader.load_available()

  $scope.open_delete_test_modal = (test_name) ->
    scope = $scope.$new true
    scope.test_name = test_name
    $modal.open
      templateUrl: 'test_delete.html'
      scope: scope
    .result.then ->
      MasterService.run_action 'delete_test',
        test_name: test_name
      TestLoader.load_available()

app.service 'TestRunner', (MasterService, Test) ->
  TestRunner =
    running: false
    run: ->
      TestRunner.running = true
      MasterService.run_action 'run_test',
        id: MasterService.id
        test: Test
    stop: ->
      MasterService.run_action 'stop_test',
        id: MasterService.id
        test: Test

  MasterService.register_action 'test_running', (data) ->
    TestRunner.running = true
  MasterService.register_action 'test_stopped', (data) ->
    TestRunner.running = false

  return TestRunner

app.service 'ResultsService', (MasterService) ->
  ResultService =
    runs: 0
    start: 0
    duration: 0
    actions: []

  MasterService.register_action 'test_result', (data) ->
    console.log 'Got result ', data

  return ResultService
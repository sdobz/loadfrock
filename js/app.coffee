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
  ws = null
  connect = ->
    ws = new WebSocket('ws://localhost:' + WEBSOCKET_PORT)
  connect()
  MasterService =
    connected: false
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

  MasterService.register_action 'error', (data) ->
    console.log 'Got error ', data['error']

  return MasterService


app.controller 'FrockBodyCtrl', ($scope, MasterService) ->
  $scope.connection = MasterService
  $scope.nav =
    test: true
    results: false

app.service 'SlaveService', (MasterService) ->
  SlaveService =
    slaves: {}
    sink_chooser: null
    kill_slave: (slave) ->
      console.log 'Killing slave', slave.slave_id
      MasterService.run_action 'quit',
        slave_id: slave.slave_id
    begin_choosing: (slave) ->
      @sink_chooser = slave
    choosing_sink: ->
      @sink_chooser != null
    choose_sink: (slave) ->
      if @sink_chooser != null
        console.log 'Setting slave(', @sink_chooser.slave_id, ')s sink to slave(', slave.slave_id, ')'
        MasterService.run_action 'set_sink',
          slave_id: @sink_chooser.slave_id
          sink_id: slave.slave_id
        @sink_chooser = null
    sink_usages: (checking_slave) ->
      usages = 0
      for id, slave of @slaves
        if slave.sink_id == checking_slave.slave_id
          usages += 1
      return usages

  calculate_sink_usages = ->
    for id, slave of SlaveService.slaves
      slave.sink_usages = 0
      slave.has_sink = false
    for id, slave of SlaveService.slaves
      if slave.sink_id
        if SlaveService.slaves[slave.sink_id]
          slave.has_sink = true
          SlaveService.slaves[slave.sink_id].sink_usages += 1


  MasterService.register_connection_action ->
    MasterService.run_action 'request_slaves'
  MasterService.register_action 'receive_slaves', (data) ->
    angular.extend SlaveService.slaves, data['slaves']
    calculate_sink_usages()

  MasterService.register_action 'slave_disconnected', (data) ->
    delete SlaveService.slaves[data['slave_id']]
    calculate_sink_usages()

  MasterService.register_action 'slave_heartbeat', (data) ->
    if data['slave_id']
      if not SlaveService.slaves[data['slave_id']]
        SlaveService.slaves[data['slave_id']] = {}
        console.log 'Slave ', data['slave_id'], 'connected'
      angular.extend SlaveService.slaves[data['slave_id']], data
      calculate_sink_usages()

  MasterService.register_action 'slave_set_sink', (data) ->
    SlaveService.slaves[data['slave_id']].sink_id = data['sink_id']
    console.log 'Slave', data['slave_id'], 'has connected to sink', data['sink_id']
    calculate_sink_usages()


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
        test: Test
    stop: ->
      MasterService.run_action 'stop_test',
        test: Test

  return TestRunner

app.service 'ResultsService', (MasterService, TestRunner) ->
  ResultService =
    clear: ->
      angular.extend ResultService,
        runs: 0
        start: 0
        stop: null
        actions: []
  ResultService.clear()

  MasterService.register_action 'test_running', (data) ->
    ResultService.clear()
    ResultService.start = new Date();
    TestRunner.running = true
  MasterService.register_action 'test_stopped', (data) ->
    ResultService.stop = new Date();
    TestRunner.running = false

  MasterService.register_action 'test_result', (data) ->
    ResultService.runs += data['total_runs']
    i = 0
    for action in data['actions']
      if not ResultService.actions[i]
        ResultService.actions.push
          runs: 0
          runs_list: []
          avg_list: []
          avg_time: 0
          name: action.name
      action_result = ResultService.actions[i]
      action_result.runs_list.push action.runs
      action_result.avg_list.push action.avg_time

      if action_result.runs == 0
        action_result.avg_time = action.avg_time
      else
        action_result.avg_time = (action_result.runs/(action_result.runs + action.runs)) * action_result.avg_time + (action.runs/(action_result.runs + action.runs)) * action.avg_time

      action_result.runs += action.runs

      runs = 0
      avg_time_data = []
      avg_data = [[action_result.runs_list[0], action_result.avg_time], [action_result.runs, action_result.avg_time]]
      for j in [0..action_result.avg_list.length]
        runs += action_result.runs_list[j]
        avg_time_data.push [runs, action_result.avg_list[j]]

      action_result.chart_data = [avg_data, avg_time_data]

      i += 1
  return ResultService

app.controller 'ResultsCtrl', ($scope, ResultsService, Test)->
  $scope.test = Test
  $scope.results = ResultsService

app.directive 'chart', ->
  restrict: 'E',
  link: (scope, elem, attrs) ->
    chart = null
    if attrs.ngOptions
      options = JSON.parse attrs.ngOptions
    else
      options = {}
    scope.$watch attrs.ngModel, (data) ->
      if !chart
        chart = $.plot elem, data, options
      else
        chart.setData data
        chart.setupGrid()
        chart.draw()

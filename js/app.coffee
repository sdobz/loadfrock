app = angular.module 'LoadFrock', []

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

app.service 'MasterService', ->
  ws = new WebSocket('ws://localhost:' + WEBSOCKET_PORT)
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
      if data == undefined
        data = {}
      data['action'] = action
      ws.send JSON.stringify data

  ws.onopen = ->
    MasterService.connected = true
    MasterService.run_action 'connect'
    for connection_action in MasterService.connection_actions
      connection_action()

  ws.onmessage = (msg) ->
    if msg
      data = JSON.parse msg.data
      if data['action']
        MasterService.actions[data['action']](data)

  return MasterService

app.controller 'FrockBodyCtrl', ($scope, $rootScope, MasterService) ->
  $scope.status = MasterService.connected

app.controller 'SlaveCtrl', ($scope, MasterService) ->
  $scope.slaves = {}

  $scope.kill_slave = (id) ->
    MasterService.run_action 'quit',
      id: id

  MasterService.register_connection_action ->
    MasterService.run_action 'request_slaves'
  MasterService.register_action 'receive_slaves', (data) ->
    $scope.$apply ->
      angular.extend $scope.slaves, data['slaves']


  MasterService.register_action 'slave_connected', (data) ->
    $scope.$apply ->
      if not $scope.slaves[data['id']]
        $scope.slaves[data['id']] = {}
      angular.extend $scope.slaves[data['id']], data

  MasterService.register_action 'slave_disconnected', (data) ->
    $scope.$apply ->
      if $scope.slaves[data['id']]
        delete $scope.slaves[data['id']]


  MasterService.register_action 'slave_heartbeat', (data) ->
    $scope.$apply ->
      if not $scope.slaves[data['id']]
        $scope.slaves[data['id']] = {}
      angular.extend $scope.slaves[data['id']], data

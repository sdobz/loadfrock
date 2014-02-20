app = angular.module 'LoadFrock', []

app.service 'MasterService', ->
  ws = new WebSocket('ws://localhost:5577/')
  MasterService =
    connected: false
    actions: {}
    register_action: (action, callback) ->
      @actions[action] = callback
    run_action: (action, data) ->
      if data == undefined
        data = {}
      data['action'] = action
      ws.send JSON.stringify data

  ws.onopen = ->
    MasterService.connected = true
    MasterService.run_action('connect')

  ws.onmessage = (msg) ->
    if msg
      data = JSON.parse(msg.data)
      if data['action']
        MasterService.actions[data['action']](data)

  return MasterService

app.controller 'FrockBodyCtrl', ($scope, $rootScope, MasterService) ->
  $scope.status = MasterService.connected

  $scope.echo_stuff = ->
    MasterService.run_action 'echo', {'message': 'stuff'}

  MasterService.register_action 'echo', (data) ->
    console.log 'Master told us to echo', data

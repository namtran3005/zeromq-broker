export default {
  'Broker_init': {
    queueName: 'Broker_init',
    frontPort: 5553,
    backPort: 5554,
    maxQueue: 10
  },
  'Broker_createTask': {
    queueName: 'Broker_createTask',
    clientType: 'dealer',
    frontPort: 5555,
    backPort: 5556,
    maxQueue: 10
  },
  'Broker_rejectTask': {
    queueName: 'Broker_rejectTask',
    frontPort: 5557,
    backPort: 5558,
    maxQueue: 10
  },
  'Broker_recover': {
    queueName: 'Broker_recover',
    clientType: 'dealer',
    frontPort: 5559,
    backPort: 5560,
    maxQueue: 10
  },
  'Broker_dispatch': {
    queueName: 'Broker_dispatch',
    clientType: 'dealer',
    frontPort: 5561,
    backPort: 5562,
    maxQueue: 10,
    doneDef: { 'neverDone': true }
  },
  'Broker_update': {
    queueName: 'Broker_update',
    clientType: 'dealer',
    frontPort: 5563,
    backPort: 5564,
    maxQueue: 10,
    doneDef: { 'neverDone': true }
  },
  'Broker_setDone': {
    queueName: 'Broker_setDone',
    clientType: 'dealer',
    frontPort: 5565,
    backPort: 5566,
    maxQueue: 10
  },
  'Broker_updateBadRep': {
    queueName: 'Broker_updateBadRep',
    clientType: 'req',
    frontPort: 5567,
    backPort: 5568,
    maxQueue: 10
  },
  'Broker_sendNext': [{
    queueName: 'Broker_sendNext1',
    clientType: 'dealer',
    nextDest: 'tcp://localhost:5571',
    frontPort: 5569,
    backPort: 5570,
    maxQueue: 10
  }, {
    queueName: 'Broker_sendNext2',
    clientType: 'dealer',
    frontPort: 5571,
    backPort: 5572,
    maxQueue: 10
  }],
  'Broker_frontendReq': [{
    queueName: 'Broker_frontendReq1',
    clientType: 'dealer',
    nextDest: 'tcp://localhost:5575',
    frontPort: 5573,
    backPort: 5574,
    maxQueue: 10,
    visibility: 5
  }, {
    queueName: 'Broker_frontendReq2',
    clientType: 'dealer',
    nextDest: 'tcp://localhost:5577',
    frontPort: 5575,
    backPort: 5576,
    maxQueue: 10,
    visibility: 5
  }, {
    queueName: 'Broker_frontendReq3',
    clientType: 'dealer',
    frontPort: 5577,
    backPort: 5578,
    maxQueue: 10,
    visibility: 5
  }],
  'Broker_dispatcher': [{
    queueName: 'Broker_dispatcher1',
    clientType: 'dealer',
    nextDest: 'tcp://localhost:5581',
    ackerUrl: 'tcp://localhost:9999',
    frontPort: 5579,
    backPort: 5580,
    maxQueue: 1000
  }, {
    queueName: 'Broker_dispatcher2',
    clientType: 'dealer',
    frontPort: 5581,
    backPort: 5582,
    maxQueue: 1000
  }]
}

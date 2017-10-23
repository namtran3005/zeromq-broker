export default {
  'Broker_init': {
    queueName: 'Broker_init',
    nextDest: 'nextBroker',
    frontPort: 5553,
    backPort: 5554,
    maxQueue: 10
  },
  'Broker_createTask': {
    queueName: 'Broker_createTask',
    nextDest: 'nextBroker',
    clientType: 'dealer',
    frontPort: 5555,
    backPort: 5556,
    maxQueue: 10
  },
  'Broker_rejectTask': {
    queueName: 'Broker_rejectTask',
    nextDest: 'nextBroker',
    frontPort: 5557,
    backPort: 5558,
    maxQueue: 10
  },
  'Broker_recover': {
    queueName: 'Broker_recover',
    nextDest: 'nextBroker',
    clientType: 'dealer',
    frontPort: 5559,
    backPort: 5560,
    maxQueue: 10
  },
  'Broker_dispatch': {
    queueName: 'Broker_dispatch',
    nextDest: 'nextBroker',
    clientType: 'dealer',
    frontPort: 5561,
    backPort: 5562,
    maxQueue: 10,
    doneDef: { 'neverDone': true }
  },
  'Broker_update': {
    queueName: 'Broker_update',
    nextDest: 'nextBroker',
    clientType: 'dealer',
    frontPort: 5563,
    backPort: 5564,
    maxQueue: 10,
    doneDef: { 'neverDone': true }
  },
  'Broker_setDone': {
    queueName: 'Broker_setDone',
    nextDest: 'nextBroker',
    clientType: 'dealer',
    frontPort: 5565,
    backPort: 5566,
    maxQueue: 10
  },
  'Broker_updateBadRep': {
    queueName: 'Broker_updateBadRep',
    nextDest: 'nextBroker',
    clientType: 'req',
    frontPort: 5567,
    backPort: 5568,
    maxQueue: 10
  }
}

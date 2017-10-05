/* @flow */
import Promise from 'bluebird';
import winston from 'winston';
import zmq from 'zeromq';
import MongoSMQ from 'mongo-message';
import Broker from './Broker.js';
import Client from './Client.js';

winston.level = 'debug';
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
  return Math.floor(Math.random() * ((max - min) + 1)) + min;
}

async function setup(options) {
  const opts = Object.assign({}, {
    queueName: 'queue1',
    nextDest: 'nextBroker',
    frontPort: 5551,
    backPort: 5552,
    maxQueue: 10,
  }, options);
  const fixtures = await (new Broker(opts)).initBroker();
  return fixtures;
}

async function teardown(fixtures) {
  return fixtures.cleanQueue().then(() => fixtures.deInitBroker());
}

test('Initiate new Broker instance', async () => {
  const brokerInstance = await setup();
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ);
  await teardown(brokerInstance);
});

test('Initiate another Broker instance should success', async () => {
  const brokerInstance = await setup();
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ);
  await teardown(brokerInstance);
});

test('An client can create a task', async (done) => {
  const brokerInstance = await setup();
  const mockFn = jest.fn().mockImplementation((msg) => {
    let strMsg = '';
    if (typeof msg === 'object' && msg) {
      strMsg = msg.toString(); // Works!
    }
    expect(strMsg).toBe("received");
    return strMsg;
  });
  const clientInst = await new Client({
    queueUrl: 'tcp://localhost:5551',
    onMessage: mockFn,
  }).init();

  clientInst.send({
    type: 'task',
    params: [Math.random()],
  });

  setTimeout(() => {
    expect(mockFn).toHaveBeenCalledTimes(1);
    clientInst.deinit();
    teardown(brokerInstance).then(done);
  }, 2000);
});

test('An client can\'t create a task when the queue is full', async (done) => {
  let intMax = getRandomInt(1, 20);
  let intReject = getRandomInt(1, 10);
  winston.debug('intMax %j', intMax);
  winston.debug('intReject %j', intReject);
  const brokerInstance = await setup({
    maxQueue: intMax,
  });
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    let strMsg = '';
    if (typeof msg === 'object' && msg) {
      strMsg = msg.toString(); // Works!
    }
    winston.debug('Index i %j', i);
    winston.debug('strMsg %j', strMsg);
    if (i >= intMax) {
      expect(strMsg).toBe("rejected");
    } else {
      expect(strMsg).toBe("received");
    }
    return strMsg;
  });

  let arrClients = [];
  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients.push(new Client({
      queueUrl: 'tcp://localhost:5551',
      onMessage: msg => mockFn(msg, i),
    }).init());
  }
  arrClients = await Promise.all(arrClients);

  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients[i].send({
      type: 'task',
      params: [Math.random()],
    });
  }

  setTimeout(() => {
    expect(mockFn).toHaveBeenCalledTimes(intMax + intReject);
    for (let i = 0; i < intMax + intReject; i += 1) {
      arrClients[i].deinit();
    }
    teardown(brokerInstance).then(done);
  }, 2000);
});


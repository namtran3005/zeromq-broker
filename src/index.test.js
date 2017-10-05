/* @flow */
import Promise from 'bluebird';
import winston from 'winston';
import zmq from 'zeromq';
import MongoSMQ from 'mongo-message';
import Broker from './Broker.js';
import Client from './Client.js';

winston.level = 'debug';
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

async function setup(options) {
  const opts = Object.assign({}, {
    queueName: 'queue1',
    nextDest: 'nextBroker',
    frontPort: 5551,
    backPort: 5552,
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

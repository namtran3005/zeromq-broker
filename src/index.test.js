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
  const fixtures = await (new Broker(options)).initBroker();
  return fixtures;
}

async function teardown(fixtures) {
  return fixtures.deInitBroker();
}

test('Initiate new Broker instance', async () => {
  const brokerInstance = await setup({
    queueName: 'queue1',
    nextDest: 'nextBroker',
    frontPort: 5551,
    backPort: 5552,
  });
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ);
  await teardown(brokerInstance);
});

test('Initiate another Broker instance should success', async () => {
  const brokerInstance = await setup({
    queueName: 'queue1',
    nextDest: 'nextBroker',
    frontPort: 5551,
    backPort: 5552,
  });
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ);
  await teardown(brokerInstance);
});

test('Initiate another Broker instance should success', async (done) => {
  const brokerInstance = await setup({
    queueName: 'queue1',
    nextDest: 'nextBroker',
    frontPort: 5551,
    backPort: 5552,
  });
  const clientInst = await new Client({ queueUrl: 'tcp://localhost:5551' }).init();
  clientInst.send({
    type: 'task',
    params: [Math.random()],
  });
  setTimeout(() => {
    clientInst.deinit();
    teardown(brokerInstance).then(done);
  }, 10000);
});

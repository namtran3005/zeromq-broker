/* @flow */
import winston from 'winston'
import MongoSMQ from 'mongo-message'
import {setup, teardown} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('Initiate new Broker instance', async () => {
  const brokerInstance = await setup(currentConfig)
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ)
  await teardown(brokerInstance)
})

test('Initiate another Broker instance should success', async () => {
  const brokerInstance = await setup(currentConfig)
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ)
  await teardown(brokerInstance)
})

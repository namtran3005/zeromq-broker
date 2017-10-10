/* @flow */
import winston from 'winston'
import Client from '../src/Client'
import {setup, teardown} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('An client can create a task successfully', async (done) => {
  const brokerInstance = await setup(currentConfig)
  const mockFn = jest.fn().mockImplementation((msg) => {
    expect(msg).toBe('received')
    expect(mockFn).toHaveBeenCalledTimes(1)
    expect(brokerInstance.numTask).toBe(1)
    clientInst.deinit()
    teardown(brokerInstance).then(done)
  })

  const clientInst = await new Client({
    queueUrl: `tcp://localhost:${currentConfig.frontPort}`,
    onMessage: mockFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })
})

test('An client can fails to create a task', async (done) => {
  const brokerInstance = await setup(currentConfig)
  brokerInstance.queueInst.createMessage = () => {
    throw new Error('error')
  }
  const mockFn = jest.fn().mockImplementation((msg) => {
    expect(msg).toBe('rejected')
    expect(mockFn).toHaveBeenCalledTimes(1)
    expect(brokerInstance.numTask).toBe(0)
    clientInst.deinit()
    teardown(brokerInstance).then(done)
  })

  const clientInst = await new Client({
    queueUrl: `tcp://localhost:${currentConfig.frontPort}`,
    onMessage: mockFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })
})

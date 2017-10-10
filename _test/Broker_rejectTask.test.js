/* @flow */
import winston from 'winston'
import Client from '../src/Client'
import {setup, teardown, getRandomInt} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('An client can\'t create a task when the queue is full', async (done) => {
  let intMax = getRandomInt(1, 20)
  let intReject = getRandomInt(1, 5)
  const brokerInstance = await setup({
    ...currentConfig,
    ...{maxQueue: intMax}
  })
  let numReceived = 0
  let numReject = 0
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    if (msg === 'rejected') {
      numReject += 1
    } else if (msg === 'received') {
      numReceived += 1
    }
    return msg
  })

  let arrClients = []
  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients.push(new Client({
      queueUrl: `tcp://localhost:${currentConfig.frontPort}`,
      onMessage: msg => mockFn(msg, i)
    }).init())
  }
  arrClients = await Promise.all(arrClients)

  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients[i].send({
      type: 'task',
      params: [Math.random()]
    })
  }

  setTimeout(() => {
    expect(mockFn).toHaveBeenCalledTimes(intMax + intReject)
    expect(numReceived).toBe(intMax)
    expect(intReject).toBe(numReject)
    for (let i = 0; i < intMax + intReject; i += 1) {
      arrClients[i].deinit()
    }
    teardown(brokerInstance).then(done)
  }, 3000)
})

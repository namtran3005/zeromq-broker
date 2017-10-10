/* @flow */
import winston from 'winston'
import Client from '../src/Client'
import {setup, teardown, repeatIn} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('A Broker instance after recover should get correct current numTask', async (done) => {
  let intMax = 100
  const brokerInstance = await setup({
    ...currentConfig,
    ...{maxQueue: intMax}
  })

  let totalClient = 0
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })
  let arrPromises = []
  let arrClients = []
  arrPromises.push(repeatIn(5000, 1000, () => brokerInstance.restart()))
  arrPromises.push(repeatIn(5000, 250, async () => {
    const numClient = 5
    for (let i = 0; i < numClient; i += 1) {
      const numId = totalClient
      totalClient += 1
      const client = await (new Client({
        queueUrl: `tcp://localhost:${currentConfig.frontPort}`,
        onMessage: msg => mockFn(msg, numId)
      })).init()
      client.send({
        type: 'task',
        params: [Math.random()]
      })
      arrClients.push(client)
    }
  }))
  Promise.all(arrPromises).then(async () => {
    expect(mockFn).toHaveBeenCalledTimes(totalClient)
    expect(brokerInstance.numTask).toBe(totalClient)
    for (let i = 0; i < totalClient; i += 1) {
      await arrClients[i].deinit()
    }
    teardown(brokerInstance).then(done)
  })
})

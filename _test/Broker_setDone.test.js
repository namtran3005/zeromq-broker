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

test('The done task should not available again', async (done) => {
  const intMax = 10
  let brokerInstance = await setup({
    ...currentConfig,
    ...{
      maxQueue: intMax,
      visibility: 1
    }
  })
  let intTries = 0
  let intHasMessage = 0
  let intNoMessage = 0
  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })

  let myFingerPrint = null
  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander (msg) {
    let objWork: any = msg
    winston.debug('Worker receive works \n', objWork)
    /* sample do work */
    if (intTries === 5) {
      expect(intHasMessage).toBe(1)
      expect(intNoMessage).toBe(intTries - intHasMessage)
      clientInst.deinit()
      workerInst.deinit()
      return teardown(brokerInstance).then(done)
    } else {
      intTries += 1
      if (objWork && objWork.message) {
        intHasMessage += 1
        myFingerPrint = `Work done Id: ${Math.random()}`
        objWork.message.result = myFingerPrint
      } else {
        intNoMessage += 1
        /* backoff before request new one */
        await repeatIn(1000, 1000, () => {})
      }
    }

    return this.send(objWork)
  })

  let clientInst = await new Client({
    queueUrl: `tcp://localhost:${currentConfig.frontPort}`,
    onMessage: mockClientFn
  }).init()

  let workerInst = await new Client({
    queueUrl: `tcp://localhost:${currentConfig.backPort}`,
    onMessage: mockWorkerFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })

  workerInst.send('')
})

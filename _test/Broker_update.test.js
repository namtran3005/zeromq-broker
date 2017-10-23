/* @flow */
import winston from 'winston'
import {setup, teardown, repeatIn, initClient} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('Broker can fails to update a task result received from worker', async (done) => {
  const intMax = 10
  let brokerInstance = await setup({
    ...currentConfig,
    ...{
      maxQueue: intMax,
      visibility: 5
    }
  })

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })

  let myFingerPrint = null
  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander (msg) {
    let objWork: any = msg
    winston.debug('Worker receive works \n', objWork)
    /* sample do work */
    if (objWork && objWork.message) {
      if (objWork.tries === 2) {
        /* eventualy it will try a second time comback here */
        clientInst.deinit()
        workerInst.deinit()
        return teardown(brokerInstance).then(done)
      }
      await repeatIn(1000, 1000, () => {})
      myFingerPrint = `Work done Id: ${Math.random()}`
      objWork.message.result = myFingerPrint
      /* this will make broker can't update the task */
      objWork.tries = objWork.tries + 1
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {})
    }
    return this.send(objWork)
  })

  let clientInst = await initClient({
    port: currentConfig.frontPort,
    socketType: currentConfig.clientType,
    onMessage: mockClientFn
  })

  let workerInst = await initClient({
    port: currentConfig.backPort,
    socketType: currentConfig.clientType,
    onMessage: mockWorkerFn
  })

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })

  workerInst.send('')
})

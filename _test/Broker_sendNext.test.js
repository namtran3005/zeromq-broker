/* @flow */
import winston from 'winston'
import {setup, teardown, repeatIn, initClient} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]

test('Broker can send a task to a next Broker', async (done) => {
  let brokerInstance1 = await setup(currentConfig[0])
  let brokerInstance2 = await setup(currentConfig[1])

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })

  let myFingerPrint = null

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander (msg) {
    let objWork: any = msg
    winston.debug('Worker receive works \n', objWork)
    /* sample do work */
    if (objWork && objWork.message) {
      myFingerPrint = `Work done Id: ${Math.random()}`
      objWork.message.result = myFingerPrint
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {})
    }
    return this.send(objWork)
  })

  const mockWorkerFn2 = jest.fn().mockImplementation(async function workerHander (msg) {
    let objWork: any = msg
    winston.debug('Worker 2 receive works \n', objWork)
    /* sample do work */
    if (objWork && objWork.message) {
      /* Eventualy it should come from broker 1 to broker 2 and stop here */
      expect(objWork.message.message.result).toBe(myFingerPrint)
      clientInst.deinit()
      workerInst.deinit()
      workerInst2.deinit()
      let p1 = await teardown(brokerInstance2)
      let p2 = await teardown(brokerInstance1)
      await p1
      await p2
      done()
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {})
    }
    return this.send(objWork)
  })

  let clientInst = await initClient({
    port: currentConfig[0].frontPort,
    socketType: currentConfig[0].clientType,
    onMessage: mockClientFn
  })

  let workerInst = await initClient({
    port: currentConfig[0].backPort,
    socketType: currentConfig[0].clientType,
    onMessage: mockWorkerFn
  })

  let workerInst2 = await initClient({
    port: currentConfig[1].backPort,
    socketType: currentConfig[1].clientType,
    onMessage: mockWorkerFn2
  })

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })

  workerInst.send(null)
  workerInst2.send(null)
})

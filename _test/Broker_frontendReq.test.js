/* @flow */
import winston from 'winston'
import {setup, teardown, repeatIn, initClient} from '../utils'
import path from 'path'
import config from './config'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

let _currentFile = path.basename(__filename, '.test.js')
const currentConfig = config[_currentFile]
const brokerConfig = {
  'doneDef': { 'message.current.result': { $exists: true } },
  'notDoneDef': { 'message.current.result': { $exists: false } },
  'onFrontReq': (req, addr) => {
    let { message: {
            current,
            msgs,
            dones
          }
    } = req || {message: {}}
    if (current && current.result) {
      dones.push(current)
    }
    current = msgs.shift()
    return {
      current,
      msgs,
      dones
    }
  },
  'onUpdateMsg': (payload) => {
    if (payload &&
        payload._id && typeof payload._id === 'string' &&
        payload.tries && typeof payload.tries === 'number' &&
        payload.message && payload.message.current.result
    ) {
      return {
        query: {
          _id: payload._id,
          tries: payload.tries
        },
        update: {
          $set: {
            'message.current.result': payload.message.current.result
          }
        }
      }
    }
  }
}

test('Broker can send list of tasks', async (done) => {
  let brokerInstance1 = await setup({
    ...currentConfig[0],
    ...brokerConfig
  })

  let brokerInstance2 = await setup({
    ...currentConfig[1],
    ...brokerConfig
  })

  let brokerInstance3 = await setup({
    ...currentConfig[2],
    ...brokerConfig
  })

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander (msg) {
    let objWork: any = msg
    winston.debug('Worker receive works \n', objWork)
    /* sample do work */
    if (objWork && objWork.message.current) {
      objWork.message.current.result = objWork.message.current.params[0] +
      objWork.message.current.params[1]
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
    if (objWork && objWork.message.current) {
      /* Eventualy it should come from broker 1 to broker 2 and stop here */
      objWork.message.current.result = objWork.message.current.params[0] + objWork.message.current.params[1]
      expect(objWork.message.current.result).toBe(11)
      clientInst.deinit()
      workerInst.deinit()
      workerInst2.deinit()
      workerInst3.deinit()
      let p1 = await teardown(brokerInstance1)
      let p2 = await teardown(brokerInstance2)
      let p3 = await teardown(brokerInstance3)
      await p1
      await p2
      await p3
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
    onMessage: mockWorkerFn
  })

  let workerInst3 = await initClient({
    port: currentConfig[2].backPort,
    socketType: currentConfig[2].clientType,
    onMessage: mockWorkerFn2
  })

  const msg = {
    'message': {
      current: {
        'params': [
          0, 1
        ],
        'type': 'task0',
        'result': 1
      },
      'msgs': [
        {
          'params': [
            1, 2
          ],
          'type': 'task1'
        }, {
          'params': [
            3, 4
          ],
          'type': 'task2'
        }, {
          'params': [
            5, 6
          ],
          'type': 'task3'
        }
      ],
      'dones': []
    }
  }
  clientInst.send(msg)
  workerInst.send(null)
  workerInst2.send(null)
  workerInst3.send(null)
})

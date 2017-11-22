/* @flow */
import winston from 'winston'
import BPromise from 'bluebird'
import ClientAck from '../src/ClientAck'
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
          },
          idx
    } = req || {message: {}}
    if (current && current.result) {
      dones.push(current)
    }
    current = msgs.shift()
    return {
      current,
      msgs,
      dones,
      idx
    }
  },
  'onFrontResp': (resp, payload) => {
    return {
      idx: payload.idx,
      result: resp
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

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHandler (msg) {
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

  let clientInst = await new ClientAck({
    queueUrl: `tcp://localhost:${currentConfig[0].frontPort}`,
    type: 'dealer',
    frontPort: 9999
  }).init()

  let workerInst = await initClient({
    port: currentConfig[0].backPort,
    socketType: currentConfig[0].clientType,
    onMessage: mockWorkerFn
  })
  const msg = {
    'message': {
      current: {
        'params': [
          0, 1
        ],
        'type': 'task1',
        'result': 1
      },
      'msgs': [
        {
          'params': [
            2, 3
          ],
          'type': 'task2'
        }, {
          'params': [
            4, 5
          ],
          'type': 'task3'
        }, {
          'params': [
            6, 7
          ],
          'type': 'task4'
        }
      ],
      'dones': []
    }
  }
  let arrPromise = []
  for (let i = 0; i < 3; i++) {
    msg.message.current.type = i
    arrPromise.push(clientInst.send(msg).then((resp) => {
      expect(resp.dones[0].type).toBe(i)
    }))
  }
  BPromise.all(arrPromise).then(async () => {
    clientInst.deinit()
    workerInst.deinit()
    await teardown(brokerInstance1)
    await teardown(brokerInstance2)
    done()
  })
  workerInst.send(null)
})

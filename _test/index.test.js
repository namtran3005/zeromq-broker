/* @flow */
import Promise from 'bluebird'
import winston from 'winston'
import MongoSMQ from 'mongo-message'
import {repeatIn, getRandomInt, setup, teardown} from '../utils'
import Client from '../src/Client'

winston.level = 'debug'
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000

test('Initiate new Broker instance', async () => {
  const brokerInstance = await setup()
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ)
  await teardown(brokerInstance)
})

test('Initiate another Broker instance should success', async () => {
  const brokerInstance = await setup()
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ)
  await teardown(brokerInstance)
})

test('An client can create a task successfully', async (done) => {
  const brokerInstance = await setup()
  const mockFn = jest.fn().mockImplementation((msg) => {
    expect(msg).toBe('received')
    expect(mockFn).toHaveBeenCalledTimes(1)
    expect(brokerInstance.numTask).toBe(1)
    clientInst.deinit()
    teardown(brokerInstance).then(done)
  })

  const clientInst = await new Client({
    queueUrl: 'tcp://localhost:5551',
    onMessage: mockFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })
})

test('An client can fails to create a task', async (done) => {
  const brokerInstance = await setup()
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
    queueUrl: 'tcp://localhost:5551',
    onMessage: mockFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })
})

test('An client can\'t create a task when the queue is full', async (done) => {
  let intMax = getRandomInt(1, 20)
  let intReject = getRandomInt(1, 5)
  const brokerInstance = await setup({
    maxQueue: intMax
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
      queueUrl: 'tcp://localhost:5551',
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

test('A Broker instance after recover should get correct current numTask', async (done) => {
  let intMax = 250
  let brokerInstance = await setup({
    maxQueue: intMax
  })

  const repeatIn = (ms: number, interval: number, cb: Function) => {
    let countDown = ms
    return new Promise((resolve) => {
      const timerId = setInterval(async () => {
        if (countDown === 0) {
          clearTimeout(timerId)
          resolve()
          return
        }
        await cb()
        countDown -= interval
      }, interval)
    })
  }

  let totalClient = 0
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    return msg
  })
  let arrPromises = []
  let arrClients = []
  arrPromises.push(repeatIn(10000, 1000, () => brokerInstance.restart()))
  arrPromises.push(repeatIn(10000, 250, async () => {
    const numClient = 5
    for (let i = 0; i < numClient; i += 1) {
      const numId = totalClient
      totalClient += 1
      const client = await (new Client({
        queueUrl: 'tcp://localhost:5551',
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

test('A Client send a task and worker received', async (done) => {
  const intMax = 10
  let brokerInstance = await setup({
    maxQueue: intMax,
    visibility: 5
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
      if (objWork.message.result === myFingerPrint) {
        /* eventualy it should go arough and comback here */
        clientInst.deinit()
        workerInst.deinit()
        return teardown(brokerInstance).then(done)
      }
      await repeatIn(1000, 1000, () => {})
      myFingerPrint = `Work done Id: ${Math.random()}`
      objWork.message.result = myFingerPrint
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {})
    }
    return this.send(objWork)
  })

  let clientInst = await new Client({
    queueUrl: 'tcp://localhost:5551',
    onMessage: mockClientFn
  }).init()

  let workerInst = await new Client({
    queueUrl: 'tcp://localhost:5552',
    onMessage: mockWorkerFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })

  workerInst.send('')
})

test('Broker can fails to update a task result received from worker', async (done) => {
  const intMax = 10
  let brokerInstance = await setup({
    maxQueue: intMax,
    visibility: 5
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

  let clientInst = await new Client({
    queueUrl: 'tcp://localhost:5551',
    onMessage: mockClientFn
  }).init()

  let workerInst = await new Client({
    queueUrl: 'tcp://localhost:5552',
    onMessage: mockWorkerFn
  }).init()

  clientInst.send({
    type: 'task',
    params: [Math.random()]
  })

  workerInst.send('')
})

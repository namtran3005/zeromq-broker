/* @flow */
import winston from 'winston'
import zeromq from 'zeromq'
import MongoSMQ from 'mongo-message'

winston.level = 'debug'

export default class Broker {
  numTask: number;
  maxQueue: number;
  queueName: string;
  visibility: number;
  nextDest: string;
  frontPort: number;
  backPort: number;
  queueInst: MongoSMQ;
  frontend : any;
  backend : any;

  constructor (options: {
    queueName: string,
    visibility: number,
    nextDest: string,
    frontPort: number,
    backPort: number,
    maxQueue: number,
  }) {
    const {
      queueName, visibility, nextDest, frontPort, backPort, maxQueue
    } = options
    this.numTask = 0
    this.maxQueue = maxQueue
    this.queueName = queueName
    this.nextDest = nextDest
    this.frontPort = frontPort
    this.backPort = backPort
    this.visibility = visibility || 30
  }

  async initQueue (): Promise<Broker> {
    this.queueInst = await (new MongoSMQ({
      colName: this.queueName,
      visibility: this.visibility
    })).init()
    this.numTask = await this.queueInst.total()
    return this
  }

  deInitQueue () {
    return this.queueInst.deinit()
  }

  cleanQueue () {
    return this.queueInst.clean()
  }

  async initBroker () {
    await this.initQueue()
    this.frontend = zeromq.socket('router')
    this.backend = zeromq.socket('router')
    this.frontend.setsockopt('linger', 0)
    this.backend.setsockopt('linger', 0)
    this.frontend.bindSync(`tcp://*:${this.frontPort}`)
    this.backend.bindSync(`tcp://*:${this.backPort}`)
    this.frontend.on('message', (...reqMsg) => {
      const [reqAddress, delimiter, payload] = reqMsg
      const parsed = JSON.parse(payload.toString())
      winston.debug('Frontend received task with info\n', {
        reqAddress: reqAddress.toString('hex'),
        delimiter: delimiter.toString(),
        payload: parsed
      })
      winston.debug(' Current number of Tasks: %d', this.numTask)
      this.receiveTask(reqAddress, delimiter, parsed)
    })
    this.backend.on('message', (...reqMsg) => {
      const [reqAddress, delimiter, payload] = reqMsg
      const parsed = JSON.parse(payload.toString())
      winston.debug('Backend received payload with info\n', {
        reqAddress: reqAddress.toString('hex'),
        delimiter: delimiter.toString(),
        payload: JSON.parse(payload.toString())
      })
      this.dispatchTask(reqAddress, delimiter, parsed)
    })
    return this
  }

  async deInitBroker () {
    this.frontend.close()
    this.backend.close()
    await this.deInitQueue()
  }

  async restart () {
    await this.deInitBroker()
    await this.initBroker()
    return this
  }

  isFull (): boolean {
    return this.numTask >= this.maxQueue
  }

  increaseTask (): void {
    this.numTask = this.numTask + 1
  }

  decreaseTask (): void {
    this.numTask = this.numTask - 1
  }

  reject (): mixed {
    winston.debug(' The queue is full, We will reject the task')
    return 'rejected'
  }

  async receive (payload : mixed): mixed {
    this.increaseTask()
    winston.debug(' The queue is not full, We will receive the task')
    winston.debug('   Current number of Tasks: %d', this.numTask)
    try {
      const resp = await this.queueInst.createMessage(payload)
      winston.debug('   New Task is created with ID:\n', resp.toString())
      return 'received'
    } catch (e) {
      this.decreaseTask()
      winston.debug('   Create Task Error\n', e.toString())
      return 'rejected'
    }
  }

  _frontEndSend (respMsg : Array<mixed>) : void {
    let [respAddress, delimiter, payload] = respMsg
    payload = payload || ''
    payload = JSON.stringify(payload)
    winston.debug('  Frontend send response\n', {
      clientAddress: (respAddress && (typeof respAddress === 'object')) ? respAddress.toString() : respAddress,
      delimiter: (delimiter && (typeof delimiter === 'object')) ? delimiter.toString() : delimiter,
      payload: payload
    })
    this.frontend.send([respAddress, delimiter, payload])
  }

  _backEndSend (respMsg : Array<mixed>) : void {
    let [respAddress, delimiter, payload] = respMsg
    payload = payload || ''
    payload = JSON.stringify(payload)
    winston.debug('  Backend send response\n', {
      workerAddress: (respAddress && (typeof respAddress === 'object')) ? respAddress.toString() : respAddress,
      delimiter: (delimiter && (typeof delimiter === 'object')) ? delimiter.toString() : delimiter,
      payload: payload
    })
    this.backend.send([respAddress, delimiter, payload])
  }

  async receiveTask (reqAddress: mixed, delimiter: mixed, payload: mixed) {
    const respMsg = [reqAddress, delimiter]
    if (this.isFull()) {
      respMsg[2] = this.reject()
    } else {
      respMsg[2] = await this.receive(payload)
    }
    return this._frontEndSend(respMsg)
  }

  async dispatchTask (reqAddress: mixed, delimiter: mixed, payload: mixed) {
    const respMsg = [reqAddress, delimiter]
    if (payload && payload.message && payload.message.result) {
      const newMsg = await this.queueInst.updateMessage(payload)
      winston.debug('  Update task with result\n', newMsg.toString())
    }
    respMsg[2] = await this.queueInst.getMessage()
    return this._backEndSend(respMsg)
  }
}

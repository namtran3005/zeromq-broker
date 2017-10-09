/* @flow */
import winston from 'winston'
import zeromq from 'zeromq'
import MongoSMQ from 'mongo-message'

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
    const isFull = this.numTask >= this.maxQueue
    winston.debug(` The queue (${this.queueName}) is${isFull ? '' : "n't"} full`)
    return isFull
  }

  increaseTask (): void {
    const after = this.numTask + 1
    winston.debug(` increaseTask \n %j`, {
      'before': this.numTask,
      'after': after
    })
    this.numTask = after
  }

  decreaseTask (): void {
    const after = this.numTask - 1
    winston.debug(` decreaseTask \n %j`, {
      'before': this.numTask,
      'after': after
    })
    this.numTask = after
  }

  reject (): mixed {
    return 'rejected'
  }

  async insertTask (payload : mixed) : mixed {
    let resp = null
    try {
      winston.debug('   Broker insert task with payload:\n %j', payload)
      resp = await this.queueInst.createMessage(payload)
      winston.debug('   Broker inserted task successfully with ID:\n', resp.toString())
    } catch (e) {
      winston.debug('   Broker inserted task fails with error:\n', e.toString())
    }
    return resp
  }

  async receive (payload : mixed): mixed {
    let respMsg = 'received'
    this.increaseTask()
    let newTask = await this.insertTask(payload)
    if (!newTask) {
      this.decreaseTask()
      respMsg = 'rejected'
    }
    return respMsg
  }

  _sendResp (respMsg : Array<mixed>, socket : any) : void {
    let [respAddress, delimiter, payload] = respMsg
    payload = JSON.stringify(payload)
    winston.debug('%j', {
      clientAddress: (respAddress && (typeof respAddress === 'object')) ? respAddress.toString('hex') : respAddress,
      delimiter: (delimiter && (typeof delimiter === 'object')) ? delimiter.toString() : delimiter,
      payload: payload
    })
    socket.send([respAddress, delimiter, payload])
  }

  async receiveTask (reqAddress: mixed, delimiter: mixed, payload: mixed) {
    const respMsg = [reqAddress, delimiter]
    if (this.isFull()) {
      respMsg[2] = this.reject()
    } else {
      respMsg[2] = await this.receive(payload)
    }
    winston.debug('  Frontend send response')
    return this._sendResp(respMsg, this.frontend)
  }

  async dispatchTask (reqAddress: mixed, delimiter: mixed, payload: mixed) {
    const respMsg = [reqAddress, delimiter]
    if (payload && payload.message && payload.message.result) {
      const newMsg = await this.queueInst.updateMessage(payload)
      winston.debug('  Update task with result\n', newMsg.toString())
    }
    respMsg[2] = await this.queueInst.getMessage()
    winston.debug('  Backend send response')
    return this._sendResp(respMsg, this.backend)
  }
}

/* @flow */
import winston from 'winston'
import zeromq from 'zeromq'
import MongoSMQ from 'mongo-message'
import Client from './Client'

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
  deliver : any;
  doneDef: any;
  notDoneDef : any;
  _timeoutFd : any;

  constructor (options: {
    queueName: string,
    visibility: number,
    nextDest: string,
    frontPort: number,
    backPort: number,
    maxQueue: number,
    doneDef : any
  }) {
    const {
      queueName, visibility, nextDest, frontPort, backPort, maxQueue, doneDef
    } = options
    this.numTask = 0
    this.maxQueue = maxQueue
    this.queueName = queueName
    this.nextDest = nextDest
    this.frontPort = frontPort
    this.backPort = backPort
    this.visibility = visibility || 30
    this.doneDef = doneDef || { 'message.result': { $exists: true } }
    this.notDoneDef = { $nor: [this.doneDef] }
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

  async registerNextDest () {
    let _sendNextAndAck = async () => {
      let doneTask = await this.queueInst.Message.find(this.doneDef)
      if (doneTask && doneTask.length !== 0) {
        let resp = await this.deliver.send(doneTask[0])
        if (resp && resp === 'received') {
          await this.queueInst.removeMessageById({
            _id: doneTask[0]._id
          })
        }
      }
      this._timeoutFd = setTimeout(_sendNextAndAck, 10000)
    }
    _sendNextAndAck()
  }

  async initBroker () {
    await this.initQueue()
    if (this.nextDest) {
      this.deliver = await new Client({
        queueUrl: this.nextDest,
        type: 'dealer'
      }).init()
      await this.registerNextDest()
    }
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
    if (this.nextDest) {
      clearTimeout(this._timeoutFd)
      this.deliver.deinit()
    }
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

  _increaseTask (): void {
    const after = this.numTask + 1
    winston.debug(` increaseTask \n %j`, {
      'before': this.numTask,
      'after': after
    })
    this.numTask = after
  }

  _decreaseTask (): void {
    const after = this.numTask - 1
    winston.debug(` decreaseTask \n %j`, {
      'before': this.numTask,
      'after': after
    })
    this.numTask = after
  }

  _reject (): mixed {
    return 'rejected'
  }

  async _insertTask (payload : mixed) : mixed {
    let resp = null
    try {
      winston.debug('   Broker insert task with payload:\n %j', payload)
      resp = await this.queueInst.createMessage(payload)
      winston.debug('   Broker inserted task successfully with ID:\n', JSON.stringify(resp))
    } catch (e) {
      winston.debug('   Broker inserted task fails with error:\n', JSON.stringify(e))
    }
    return resp
  }

  _checkObjResult (payload: mixed) : {
    _id : string,
    tries : number,
    message : { result : mixed }
  } | void {
    let updateParam
    if (payload &&
        payload._id && typeof payload._id === 'string' &&
        payload.tries && typeof payload.tries === 'number' &&
        payload.message && payload.message.result
    ) {
      updateParam = {
        _id: payload._id,
        tries: payload.tries,
        message: {
          result: payload.message.result
        }
      }
    }
    return updateParam
  }

  async _updateTask (payload : mixed) : mixed {
    let resp
    try {
      winston.debug('   Broker update task with payload:\n %j', payload)
      let updateParam = this._checkObjResult(payload)
      if (updateParam == null) {
        throw Error('Update payload is invalid')
      } else {
        resp = await this.queueInst.updateMessage({
          _id: updateParam._id,
          tries: updateParam.tries,
          message: {
            result: updateParam.message.result
          }
        })
      }
      if (resp) {
        winston.debug('   Broker update task successfully with ID:\n', JSON.stringify(resp))
      } else {
        throw Error('Fails to update task')
      }
    } catch (e) {
      winston.debug('   Broker update task fails with error:\n', e.message)
    }
    return resp
  }

  async _receive (payload : mixed): mixed {
    let respMsg = 'received'
    this._increaseTask()
    let newTask = await this._insertTask(payload)
    if (!newTask) {
      this._decreaseTask()
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
      respMsg[2] = this._reject()
    } else {
      respMsg[2] = await this._receive(payload)
    }
    winston.debug('  Frontend send response')
    return this._sendResp(respMsg, this.frontend)
  }

  async dispatchTask (reqAddress: mixed, delimiter: mixed, payload: mixed) {
    const respMsg = [reqAddress, delimiter]
    await this._updateTask(payload)
    respMsg[2] = await this.queueInst.getMessage(this.notDoneDef)
    winston.debug('  Backend send response')
    return this._sendResp(respMsg, this.backend)
  }
}

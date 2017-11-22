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
  ackerUrl : any;
  _acker:any;
  doneDef: any;
  notDoneDef : any;
  _timeoutFd : any;
  onFrontReq : (mixed, mixed) => any;
  onFrontResp : (mixed, mixed) => any;
  onBackReq : (mixed, mixed) => any;
  onBackResp : (mixed, mixed) => any;
  onUpdateMsg: (payload: mixed) => {
    query : { _id : string, tries : number},
    update : any
  } | void;

  constructor (options: {
    queueName: string,
    visibility: number,
    nextDest: string,
    frontPort: number,
    backPort: number,
    maxQueue: number,
    doneDef : any,
    ackerUrl: string,
    onFrontReq : (mixed, mixed) => any,
    onFrontResp : (mixed, mixed) => any,
    onBackReq : (mixed, mixed) => any,
    onBackResp : (mixed, mixed) => any,
    onUpdateMsg: (payload: mixed) => {
      query : { _id : string, tries : number},
      update : any
    } | void;
  }) {
    const {
      queueName, visibility, nextDest, frontPort, backPort, maxQueue, doneDef,
      onFrontReq, onFrontResp, onBackReq, onBackResp, onUpdateMsg, ackerUrl
    } = options
    this.numTask = 0
    this.maxQueue = maxQueue
    this.queueName = queueName
    this.nextDest = nextDest
    this.frontPort = frontPort
    this.backPort = backPort
    this.ackerUrl = ackerUrl
    this.visibility = visibility || 30
    this.doneDef = doneDef || { 'message.result': { $exists: true } }
    this.notDoneDef = { $nor: [this.doneDef] }
    this.onFrontReq = onFrontReq
    this.onFrontResp = onFrontResp
    this.onBackReq = onBackReq
    this.onBackResp = onBackResp
    this.onUpdateMsg = onUpdateMsg || this._onUpdateMsg
  }

  async initQueue () {
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
      winston.debug('Deliver retrived a done work with info %j\n', doneTask[0])
      if (doneTask && doneTask.length !== 0) {
        let resp = await this.deliver.send(doneTask[0])
        if (resp && ((resp.result === 'received') || resp === 'received')) {
          winston.debug('Task sended successfully %j\n', resp)
          if (this._acker) {
            let ackResp = await this._acker.send(doneTask[0].message)
            winston.debug('Task acked successfully %j\n', ackResp)
          }
          await this.queueInst.removeMessageById({
            _id: doneTask[0]._id
          })
        }
      }
      this._timeoutFd = setTimeout(_sendNextAndAck, 1000)
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
    if (this.ackerUrl) {
      this._acker = await new Client({
        queueUrl: this.ackerUrl,
        type: 'dealer'
      }).init()
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
      const req = this.onFrontReq ? this.onFrontReq(parsed, reqAddress) : parsed
      this.receiveTask(reqAddress, delimiter, req)
    })
    this.backend.on('message', (...reqMsg) => {
      const [reqAddress, delimiter, payload] = reqMsg
      const parsed = JSON.parse(payload.toString())
      winston.debug('Backend received payload with info\n', {
        reqAddress: reqAddress.toString('hex'),
        delimiter: delimiter.toString(),
        payload: parsed
      })
      const req = this.onBackReq ? this.onBackReq(parsed, reqAddress) : parsed
      this.dispatchTask(reqAddress, delimiter, req)
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

  _onUpdateMsg (payload: mixed) : {
    query : { _id : string, tries : number},
    update : any
  } | void {
    if (payload &&
        payload._id && typeof payload._id === 'string' &&
        payload.tries && typeof payload.tries === 'number' &&
        payload.message && payload.message.result
    ) {
      return {
        query: {
          _id: payload._id,
          tries: payload.tries
        },
        update: {
          $set: {
            'message.result': payload.message.result
          }
        }
      }
    }
  }

  async _updateTask (payload : mixed) : mixed {
    let resp
    try {
      winston.debug('   Broker update task with payload:\n %j', payload)
      let updateParam = this.onUpdateMsg(payload)
      if (updateParam == null) {
        throw Error('Update payload is invalid')
      } else {
        resp = await this.queueInst.updateMessage(updateParam.query, updateParam.update)
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
    let resp
    if (this.isFull()) {
      resp = this._reject()
    } else {
      resp = await this._receive(payload)
    }
    if (this.onFrontResp) {
      /* TODO this params passing is confusing */
      resp = this.onFrontResp(resp, payload)
    }
    respMsg[2] = resp
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

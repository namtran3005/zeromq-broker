/* @flow */
import zeromq from 'zeromq'
import BPromise from 'bluebird'
import uuid from 'uuid'
import winston from 'winston'

export default class ClientAck {
  queueUrl: string;
  onMessage: (payload: any) => any;
  requester: any;
  acker: any;
  frontPort: number;
  _type : string;
  _promises : {
    [string] : {
      resolve: (value: any) => any,
      reject: (value: any) => any
    }
  };

  constructor (options: {
    queueUrl: string,
    frontPort: number,
    type?: ?string,
    onMessage: (msg: mixed) => any,
  }) {
    const {
      queueUrl,
      onMessage,
      type,
      frontPort
    } = options
    this.queueUrl = queueUrl
    this.onMessage = onMessage
    this.frontPort = frontPort
    this._type = type || 'req'
    this._promises = {}
  }

  async init () {
    this.requester = zeromq.socket(this._type)
    this.requester.connect(this.queueUrl)
    this.requester.setsockopt('linger', 0)
    this.requester.on('message', (...args) => {
      let data = this._type === 'req' ? args[0] : args[1]
      let parsedRep = JSON.parse(data.toString())
      if (this.onMessage) {
        this.onMessage(parsedRep)
      }
      if (!this.frontPort) {
        this._promises[parsedRep.idx].resolve(parsedRep)
      }
    })

    if (this.frontPort) {
      this.acker = zeromq.socket('router')
      this.acker.setsockopt('linger', 0)
      this.acker.bindSync(`tcp://*:${this.frontPort}`)
      this.acker.on('message', (...reqMsg) => {
        const [reqAddress, delimiter, payload] = reqMsg
        const parsed = JSON.parse(payload.toString())
        winston.debug('Acker received ack with info\n', {
          reqAddress: reqAddress.toString('hex'),
          delimiter: delimiter.toString(),
          payload: parsed
        })
        const resp = this.onFrontReq ? this.onFrontReq(parsed, reqAddress) : parsed
        this._promises[parsed.idx].resolve(resp)
        this._sendResp([reqAddress, delimiter, true], this.acker)
      })
    }
    return this
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

  async deinit () {
    return this.requester.close()
  }

  send (payload: any) {
    return new BPromise((resolve, reject) => {
      let idx = uuid.v1()
      this._promises[idx] = {
        idx,
        resolve,
        reject
      }
      payload.idx = idx
      let send = JSON.stringify(payload)
      let data = this._type === 'req' ? send : ['', send]
      this.requester.send(data)
    })
  }
}

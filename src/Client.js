/* @flow */
import zeromq from 'zeromq'
import BPromise from 'bluebird'

export default class Client {
  queueUrl: string;
  onMessage: (payload: any) => any;
  requester: any;
  _type : string;
  _resolve;
  _reject;

  constructor (options: {
    queueUrl: string,
    type?: ?string,
    onMessage: (msg: mixed) => any,
  }) {
    const {
      queueUrl,
      onMessage,
      type
    } = options
    this.queueUrl = queueUrl
    this.onMessage = onMessage
    this._type = type || 'req'
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
      this._resolve(parsedRep)
    })
    return this
  }

  async deinit () {
    return this.requester.close()
  }

  send (payload: any) {
    return new BPromise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
      let send = JSON.stringify(payload)
      let data = this._type === 'req' ? send : ['', send]
      this.requester.send(data)
    })
  }
}

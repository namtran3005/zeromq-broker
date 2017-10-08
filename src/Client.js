/* @flow */
import zeromq from 'zeromq'

export default class Client {
  queueUrl: string;
  onMessage: (payload: any) => any;
  requester: any;
  constructor (options: {
    queueUrl: string,
    onMessage: (msg: mixed) => any,
  }) {
    const {
      queueUrl,
      onMessage
    } = options
    this.queueUrl = queueUrl
    this.onMessage = onMessage
  }

  async init (): Promise<Client> {
    this.requester = zeromq.socket('req')
    this.requester.connect(this.queueUrl)
    this.requester.setsockopt('linger', 0)
    this.requester.on('message', (payload) => this.onMessage(JSON.parse(payload.toString())))
    return this
  }

  async deinit () {
    return this.requester.close()
  }

  send (payload: any) {
    this.requester.send(JSON.stringify(payload))
  }
}

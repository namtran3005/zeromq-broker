/* @flow */
import zeromq from 'zeromq';

export default class Client {
  queueUrl: string;
  constructor(options: {
    queueUrl: string
  }) {
    const {
      queueUrl
    } = options;
    this.queueUrl = queueUrl;
  }

  async init(): Promise<Client> {
    this.requester = zeromq.socket('req');
    this.requester.connect(this.queueUrl);
    this.requester.setsockopt('linger', 0);
    this.requester.on('message', (msg) => { console.log('got reply', msg.toString()); });
    return this;
  }

  async deinit() {
    return this.requester.close();
  }

  send(payload) {
    this.requester.send(JSON.stringify(payload));
  }

}

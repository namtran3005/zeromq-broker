/* @flow */
import zeromq from 'zeromq';

export default class Client {
  queueUrl: string;
  constructor(options: {
    queueUrl: string,
    onMessage: (msg: mixed) => string,
  }) {
    const {
      queueUrl,
      onMessage,
    } = options;
    this.queueUrl = queueUrl;
    this.onMessage = onMessage;
  }

  async init(): Promise<Client> {
    this.requester = zeromq.socket('req');
    this.requester.connect(this.queueUrl);
    this.requester.setsockopt('linger', 0);
    this.requester.on('message', this.onMessage);
    return this;
  }

  async deinit() {
    return this.requester.close();
  }

  send(payload) {
    this.requester.send(JSON.stringify(payload));
  }

}

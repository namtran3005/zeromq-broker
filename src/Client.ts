/* @flow */
import * as BPromise from "bluebird";
import * as zeromq from "zeromq";

export default class Client {
  public queueUrl: string;
  public onMessage?: (payload: any) => any;
  public requester: any;
  public type: string;
  public resolve;
  public reject;

  constructor(options: {
    queueUrl: string,
    type?: string,
    onMessage?: (msg: any) => any,
  }) {
    const {
      queueUrl,
      onMessage,
      type,
    } = options;
    this.queueUrl = queueUrl;
    this.onMessage = onMessage;
    this.type = type || "req";
  }

  public async init() {
    this.requester = zeromq.socket(this.type);
    this.requester.connect(this.queueUrl);
    this.requester.setsockopt("linger", 0);
    this.requester.on("message", (...args) => {
      const data = this.type === "req" ? args[0] : args[1];
      const parsedRep = JSON.parse(data.toString());
      if (this.onMessage) {
        this.onMessage(parsedRep);
      }
      this.resolve(parsedRep);
    });
    return this;
  }

  public async deinit() {
    return this.requester.close();
  }

  public send(payload: any) {
    return new BPromise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
      const send = JSON.stringify(payload);
      const data = this.type === "req" ? send : ["", send];
      this.requester.send(data);
    });
  }
}

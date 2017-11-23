import * as BPromise from "bluebird";
import * as uuid from "uuid";
import * as winston from "winston";
import * as zeromq from "zeromq";

export default class ClientAck {
  public queueUrl: string;
  public onMessage?: (payload: any) => any;
  public requester: any;
  public acker: any;
  public frontPort: number;
  public _type: string;
  public _promises: {
    [string]: {
      resolve: (value: any) => any,
      reject: (value: any) => any,
    },
  };

  constructor(options: {
    queueUrl: string,
    frontPort: number,
    type?: string,
    onMessage?: (msg: any) => any,
  }) {
    const {
      queueUrl,
      onMessage,
      type,
      frontPort,
    } = options;
    this.queueUrl = queueUrl;
    this.onMessage = onMessage;
    this.frontPort = frontPort;
    this._type = type || "req";
    this._promises = {};
  }

  public async init() {
    this.requester = zeromq.socket(this._type);
    this.requester.connect(this.queueUrl);
    this.requester.setsockopt("linger", 0);
    this.requester.on("message", (...args) => {
      const data = this._type === "req" ? args[0] : args[1];
      const parsedRep = JSON.parse(data.toString());
      if (this.onMessage) {
        this.onMessage(parsedRep);
      }
      if (!this.frontPort) {
        this._promises[parsedRep.idx].resolve(parsedRep);
      }
    });

    if (this.frontPort) {
      this.acker = zeromq.socket("router");
      this.acker.setsockopt("linger", 0);
      this.acker.bindSync(`tcp://*:${this.frontPort}`);
      this.acker.on("message", (...reqMsg) => {
        const [reqAddress, delimiter, payload] = reqMsg;
        const parsed = JSON.parse(payload.toString());
        winston.debug("Acker received ack with info\n", {
          reqAddress: reqAddress.toString("hex"),
          delimiter: delimiter.toString(),
          payload: parsed,
        });
        const resp = this.onFrontReq ? this.onFrontReq(parsed, reqAddress) : parsed;
        this._promises[parsed.idx].resolve(resp);
        this._sendResp([reqAddress, delimiter, true], this.acker);
      });
    }
    return this;
  }

  public _sendResp(respMsg: any[], socket: any): void {
    let [respAddress, delimiter, payload] = respMsg;
    payload = JSON.stringify(payload);
    winston.debug("%j", {
      clientAddress: (respAddress && (typeof respAddress === "object")) ? respAddress.toString("hex") : respAddress,
      delimiter: (delimiter && (typeof delimiter === "object")) ? delimiter.toString() : delimiter,
      payload,
    });
    socket.send([respAddress, delimiter, payload]);
  }

  public async deinit() {
    return this.requester.close();
  }

  public send(payload: any) {
    return new BPromise((resolve, reject) => {
      const idx = uuid.v1();
      this._promises[idx] = {
        idx,
        reject,
        resolve,
      };
      payload.idx = idx;
      const send = JSON.stringify(payload);
      const data = this._type === "req" ? send : ["", send];
      this.requester.send(data);
    });
  }
}

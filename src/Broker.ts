import MongoSMQ from "mongo-message";
import * as winston from "winston";
import * as zeromq from "zeromq";
import Client from "./Client";

export default class Broker {
  public numTask: number;
  public maxQueue: number;
  public queueName: string;
  public visibility: number;
  public nextDest: string;
  public frontPort: number;
  public backPort: number;
  public queueInst: MongoSMQ;
  public frontend: any;
  public backend: any;
  public deliver: any;
  public ackerUrl: any;
  public acker: any;
  public doneDef: any;
  public notDoneDef: any;
  public timeoutFd: any;
  public onFrontReq: (any, any) => any;
  public onFrontResp: (any, any) => any;
  public onBackReq: (any, any) => any;
  public onBackResp: (any, any) => any;
  public onUpdateMsg: (payload: any) => {
    query: { _id: string, tries: number},
    update: any,
  } | void;

  constructor(options: {
    queueName: string,
    visibility: number,
    nextDest: string,
    frontPort: number,
    backPort: number,
    maxQueue: number,
    doneDef: any,
    ackerUrl: string,
    onFrontReq: (any, any) => any,
    onFrontResp: (any, any) => any,
    onBackReq: (any, any) => any,
    onBackResp: (any, any) => any,
    onUpdateMsg: (payload: any) => {
      query: { _id: string, tries: number},
      update: any,
    } | void;
  }) {
    const {
      queueName, visibility, nextDest, frontPort, backPort, maxQueue, doneDef,
      onFrontReq, onFrontResp, onBackReq, onBackResp, onUpdateMsg, ackerUrl,
    } = options;
    this.numTask = 0;
    this.maxQueue = maxQueue;
    this.queueName = queueName;
    this.nextDest = nextDest;
    this.frontPort = frontPort;
    this.backPort = backPort;
    this.ackerUrl = ackerUrl;
    this.visibility = visibility || 30;
    this.doneDef = doneDef || { "message.result": { $exists: true } };
    this.notDoneDef = { $nor: [this.doneDef] };
    this.onFrontReq = onFrontReq;
    this.onFrontResp = onFrontResp;
    this.onBackReq = onBackReq;
    this.onBackResp = onBackResp;
    this.onUpdateMsg = onUpdateMsg || this._onUpdateMsg;
  }

  public async initQueue() {
    this.queueInst = await (new MongoSMQ({
      colName: this.queueName,
      visibility: this.visibility,
    })).init();
    this.numTask = await this.queueInst.total();
    return this;
  }

  public deInitQueue() {
    return this.queueInst.deinit();
  }

  public cleanQueue() {
    return this.queueInst.clean();
  }

  public async registerNextDest() {
    const sendNextAndAck = async () => {
      const doneTask = await this.queueInst.Message.find(this.doneDef);
      winston.debug("Deliver retrived a done work with info %j\n", doneTask[0]);
      if (doneTask && doneTask.length !== 0) {
        const resp = await this.deliver.send(doneTask[0]);
        if (resp && ((resp.result === "received") || resp === "received")) {
          winston.debug("Task sended successfully %j\n", resp);
          if (this.acker) {
            const ackResp = await this.acker.send(doneTask[0].message);
            winston.debug("Task acked successfully %j\n", ackResp);
          }
          await this.queueInst.removeMessageById({
            _id: doneTask[0]._id,
          });
        }
      }
      this.timeoutFd = setTimeout(sendNextAndAck, 1000);
    };
    sendNextAndAck();
  }

  public async initBroker() {
    await this.initQueue();
    if (this.nextDest) {
      this.deliver = await new Client({
        queueUrl: this.nextDest,
        type: "dealer",
      }).init();
      await this.registerNextDest();
    }
    if (this.ackerUrl) {
      this.acker = await new Client({
        queueUrl: this.ackerUrl,
        type: "dealer",
      }).init();
    }
    this.frontend = zeromq.socket("router");
    this.backend = zeromq.socket("router");
    this.frontend.setsockopt("linger", 0);
    this.backend.setsockopt("linger", 0);
    this.frontend.bindSync(`tcp://*:${this.frontPort}`);
    this.backend.bindSync(`tcp://*:${this.backPort}`);
    this.frontend.on("message", (...reqMsg) => {
      const [reqAddress, delimiter, payload] = reqMsg;
      const parsed = JSON.parse(payload.toString());
      winston.debug("Frontend received task with info\n", {
        delimiter: delimiter.toString(),
        payload: parsed,
        reqAddress: reqAddress.toString("hex"),
      });
      const req = this.onFrontReq ? this.onFrontReq(parsed, reqAddress) : parsed;
      this.receiveTask(reqAddress, delimiter, req);
    });
    this.backend.on("message", (...reqMsg) => {
      const [reqAddress, delimiter, payload] = reqMsg;
      const parsed = JSON.parse(payload.toString());
      winston.debug("Backend received payload with info\n", {
        delimiter: delimiter.toString(),
        payload: parsed,
        reqAddress: reqAddress.toString("hex"),
      });
      const req = this.onBackReq ? this.onBackReq(parsed, reqAddress) : parsed;
      this.dispatchTask(reqAddress, delimiter, req);
    });
    return this;
  }

  public async deInitBroker() {
    this.frontend.close();
    this.backend.close();
    if (this.nextDest) {
      clearTimeout(this.timeoutFd);
      this.deliver.deinit();
    }
    await this.deInitQueue();
  }

  public async restart() {
    await this.deInitBroker();
    await this.initBroker();
    return this;
  }

  public isFull(): boolean {
    const isFull = this.numTask >= this.maxQueue;
    winston.debug(` The queue (${this.queueName}) is${isFull ? "" : "n't"} full`);
    return isFull;
  }

  public _increaseTask(): void {
    const after = this.numTask + 1;
    winston.debug(` increaseTask \n %j`, {
      after,
      before: this.numTask,
    });
    this.numTask = after;
  }

  public _decreaseTask(): void {
    const after = this.numTask - 1;
    winston.debug(` decreaseTask \n %j`, {
      after,
      before: this.numTask,
    });
    this.numTask = after;
  }

  public _reject(): any {
    return "rejected";
  }

  public async _insertTask(payload: any): any {
    let resp = null;
    try {
      winston.debug("   Broker insert task with payload:\n %j", payload);
      resp = await this.queueInst.createMessage(payload);
      winston.debug("   Broker inserted task successfully with ID:\n", JSON.stringify(resp));
    } catch (e) {
      winston.debug("   Broker inserted task fails with error:\n", JSON.stringify(e));
    }
    return resp;
  }

  public _onUpdateMsg(payload: any): {
    query: { _id: string, tries: number},
    update: any,
  } | void {
    if (payload &&
        payload._id && typeof payload._id === "string" &&
        payload.tries && typeof payload.tries === "number" &&
        payload.message && payload.message.result
    ) {
      return {
        query: {
          _id: payload._id,
          tries: payload.tries,
        },
        update: {
          $set: {
            "message.result": payload.message.result,
          },
        },
      };
    }
  }

  public async _updateTask(payload: any): any {
    let resp;
    try {
      winston.debug("   Broker update task with payload:\n %j", payload);
      const updateParam = this.onUpdateMsg(payload);
      if (updateParam == null) {
        throw Error("Update payload is invalid");
      } else {
        resp = await this.queueInst.updateMessage(updateParam.query, updateParam.update);
      }
      if (resp) {
        winston.debug("   Broker update task successfully with ID:\n", JSON.stringify(resp));
      } else {
        throw Error("Fails to update task");
      }
    } catch (e) {
      winston.debug("   Broker update task fails with error:\n", e.message);
    }
    return resp;
  }

  public async _receive(payload: any): any {
    let respMsg = "received";
    this._increaseTask();
    const newTask = await this._insertTask(payload);
    if (!newTask) {
      this._decreaseTask();
      respMsg = "rejected";
    }
    return respMsg;
  }

  public _sendResp(respMsg: any[], socket: any): void {
    const [respAddress, delimiter] = respMsg;
    const payload = JSON.stringify(respMsg[2]);
    winston.debug("%j", {
      clientAddress: (respAddress && (typeof respAddress === "object")) ? respAddress.toString("hex") : respAddress,
      delimiter: (delimiter && (typeof delimiter === "object")) ? delimiter.toString() : delimiter,
      payload,
    });
    socket.send([respAddress, delimiter, payload]);
  }

  public async receiveTask(reqAddress: any, delimiter: any, payload: any) {
    const respMsg = [reqAddress, delimiter];
    let resp;
    if (this.isFull()) {
      resp = this._reject();
    } else {
      resp = await this._receive(payload);
    }
    if (this.onFrontResp) {
      /* TODO this params passing is confusing */
      resp = this.onFrontResp(resp, payload);
    }
    respMsg[2] = resp;
    winston.debug("  Frontend send response");
    return this._sendResp(respMsg, this.frontend);
  }

  public async dispatchTask(reqAddress: any, delimiter: any, payload: any) {
    const respMsg = [reqAddress, delimiter];
    await this._updateTask(payload);
    respMsg[2] = await this.queueInst.getMessage(this.notDoneDef);
    winston.debug("  Backend send response");
    return this._sendResp(respMsg, this.backend);
  }
}

/* @flow */
import Promise from 'bluebird';
import winston from 'winston';
import zeromq from 'zeromq';
import MongoSMQ from 'mongo-message';

winston.level = 'debug';

export default class Broker {
  numTask: number;
  MAXQUEUE: number;
  queueName: string;
  nextDest: string;
  frontPort: number;
  backPort: number;
  queueInst: MongoSMQ;

  constructor(options: {
    queueName: string, nextDest: string,
    frontPort: number, backPort: number
  }) {
    const {
      queueName, nextDest, frontPort, backPort,
    } = options;
    this.numTask = 0;
    this.MAXQUEUE = 4;
    this.queueName = queueName;
    this.nextDest = nextDest;
    this.frontPort = frontPort;
    this.backPort = backPort;
  }

  async initQueue(): Promise<Broker> {
    this.queueInst = await (new MongoSMQ()).init();
    return this;
  }

  async deInitQueue() {
    return this.queueInst.deinit();
  }

  async initBroker() {
    await this.initQueue();
    this.frontend = zeromq.socket('router');
    this.backend = zeromq.socket('router');
    this.frontend.bindSync(`tcp://*:${this.frontPort}`);
    this.backend.bindSync(`tcp://*:${this.backPort}`);
    this.frontend.on('message', (...reqMsg) => {
      this.receiveTask(...reqMsg);
    });
    this.backend.on('message', (...reqMsg) => {
      this.dispatchTask(...reqMsg);
    });
    return this;
  }

  async deInitBroker() {
    this.frontend.close();
    this.backend.close();
    await this.deInitQueue();
  }

  async receiveTask(...reqMsg) {
    // Note that separate message parts come as function arguments.
    const [reqAddress, delimiter, payload] = reqMsg;

    winston.debug('*** Frondend received task with info', {
      reqAddress: reqAddress.toString('hex'),
      delimiter: reqAddress.toString(),
    });
    winston.debug('*** Current number of Tasks: %d', this.numTask);

    const respMsg = [reqAddress, delimiter];
    const objWork = JSON.parse(payload.toString());
    winston.debug('****** Received payload', objWork);

    if (this.numTask >= this.MAXQUEUE) {
      winston.debug('****** The queue is full, We will reject the task');
      respMsg[2] = 'rejected';
    } else {
      this.numTask = this.numTask + 1;
      winston.debug('****** The queue is not full, We will receive the task');
      winston.debug('*** Current number of Tasks: %d', this.numTask);

      const resp = await this.queueInst.createMessage(objWork);
      winston.debug('****** New Task is created with ID:', resp.toString());
      respMsg[2] = 'received';
    }

    winston.info('*** Send response', {
      respAddress: respMsg[0].toString('hex'),
      delimiter: respMsg[1].toString(),
      respPayload: respMsg[2],
    });

    return this.frontend.send(respMsg);
  }
}

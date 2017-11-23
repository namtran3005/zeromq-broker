/* @flow */
import * as path from "path";
import * as winston from "winston";
import {initClient, repeatIn, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("Broker can send a task to a next Broker", async (done) => {
  const brokerInstance1 = await setup(currentConfig[0]);
  const brokerInstance2 = await setup(currentConfig[1]);

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });

  let myFingerPrint = null;

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander(msg) {
    const objWork: any = msg;
    winston.debug("Worker receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message) {
      myFingerPrint = `Work done Id: ${Math.random()}`;
      objWork.message.result = myFingerPrint;
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {/*Do no things*/});
    }
    return this.send(objWork);
  });

  const mockWorkerFn2 = jest.fn().mockImplementation(async function workerHander(msg) {
    const objWork: any = msg;
    winston.debug("Worker 2 receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message) {
      /* Eventualy it should come from broker 1 to broker 2 and stop here */
      expect(objWork.message.message.result).toBe(myFingerPrint);
      clientInst.deinit();
      workerInst.deinit();
      workerInst2.deinit();
      const p1 = await teardown(brokerInstance2);
      const p2 = await teardown(brokerInstance1);
      await p1;
      await p2;
      done();
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {/*Do no things*/});
    }
    return this.send(objWork);
  });

  const clientInst = await initClient({
    onMessage: mockClientFn,
    port: currentConfig[0].frontPort,
    socketType: currentConfig[0].clientType,
  });

  const workerInst = await initClient({
    onMessage: mockWorkerFn,
    port: currentConfig[0].backPort,
    socketType: currentConfig[0].clientType,
  });

  const workerInst2 = await initClient({
    onMessage: mockWorkerFn2,
    port: currentConfig[1].backPort,
    socketType: currentConfig[1].clientType,
  });

  clientInst.send({
    params: [Math.random()],
    type: "task",
  });

  workerInst.send(null);
  workerInst2.send(null);
});

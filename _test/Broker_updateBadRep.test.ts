import * as path from "path";
import * as winston from "winston";
import {initClient, repeatIn, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("Return a wrong payload cause broker fails to update", async (done) => {
  const intMax = 10;
  const brokerInstance = await setup({
    ...currentConfig,
    ...{
      maxQueue: intMax,
      visibility: 5,
    },
  });

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });

  let myFingerPrint = null;
  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander(msg) {
    let objWork: any = msg;
    winston.debug("Worker receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message) {
      if (objWork.tries === 2) {
        /* eventualy it will try a second time comback here */
        expect(objWork.message.params[0]).toBe(myFingerPrint);
        clientInst.deinit();
        workerInst.deinit();
        return teardown(brokerInstance).then(done);
      }
      await repeatIn(1000, 1000, () => {/* Do nothing */});
      myFingerPrint = objWork.message.params[0];
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {/* Do nothing */});
    }
    objWork = null;
    return this.send(objWork);
  });

  const clientInst = await initClient({
    onMessage: mockClientFn,
    port: currentConfig.frontPort,
    socketType: currentConfig.clientType,
  });

  const workerInst = await initClient({
    onMessage: mockWorkerFn,
    port: currentConfig.backPort,
    socketType: currentConfig.clientType,
  });

  clientInst.send({
    params: [Math.random()],
    type: "task",
  });

  workerInst.send("");
});

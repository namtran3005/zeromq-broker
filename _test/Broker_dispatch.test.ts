import * as path from "path";
import * as winston from "winston";
import {initClient, setup, sleep, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("A Client send a task and worker received", async (done) => {
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
    const objWork: any = msg;
    winston.debug("Worker receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message) {
      if (objWork.message.result === myFingerPrint) {
        /* eventualy it should go arough and comback here */
        clientInst.deinit();
        workerInst.deinit();
        return teardown(brokerInstance).then(done);
      }
      await sleep(1000);
      myFingerPrint = `Work done Id: ${Math.random()}`;
      objWork.message.result = myFingerPrint;
    } else {
      /* backoff before request new one */
      await sleep(1000);
    }
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

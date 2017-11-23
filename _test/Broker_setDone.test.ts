import * as path from "path";
import * as winston from "winston";
import {initClient, repeatIn, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("The done task should not available again", async (done) => {
  const intMax = 10;
  const brokerInstance = await setup({
    ...currentConfig,
    ...{
      maxQueue: intMax,
      visibility: 1,
    },
  });
  let intTries = 0;
  let intHasMessage = 0;
  let intNoMessage = 0;
  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });

  let myFingerPrint = null;
  const mockWorkerFn = jest.fn().mockImplementation(async function workerHandler(msg) {
    const objWork: any = msg;
    winston.debug("Worker receive works \n", objWork);
    /* sample do work */
    if (intTries === 5) {
      expect(intHasMessage).toBe(1);
      expect(intNoMessage).toBe(intTries - intHasMessage);
      clientInst.deinit();
      workerInst.deinit();
      return teardown(brokerInstance).then(done);
    } else {
      intTries += 1;
      if (objWork && objWork.message) {
        intHasMessage += 1;
        myFingerPrint = `Work done Id: ${Math.random()}`;
        objWork.message.result = myFingerPrint;
      } else {
        intNoMessage += 1;
        /* backoff before request new one */
        await repeatIn(1000, 1000, () => {/*Do nothings*/});
      }
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

  workerInst.send(null);
});

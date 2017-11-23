import * as path from "path";
import * as winston from "winston";
import {initClient, repeatIn, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];
const brokerConfig = {
  doneDef: { "message.current.result": { $exists: true } },
  notDoneDef: { "message.current.result": { $exists: false } },
  onFrontReq: (req, addr) => {
    const {
      message: {
            current,
            dones,
            msgs,
      },
    } = req || {
      message: {
      current: {},
      dones: [],
      msgs: [],
    }};
    if (current && current.result) {
      dones.push(current);
    }
    return {
      current: msgs.shift(),
      dones,
      msgs,
    };
  },
  onUpdateMsg: (payload) => {
    if (payload &&
        payload._id && typeof payload._id === "string" &&
        payload.tries && typeof payload.tries === "number" &&
        payload.message && payload.message.current.result
    ) {
      return {
        query: {
          _id: payload._id,
          tries: payload.tries,
        },
        update: {
          $set: {
            "message.current.result": payload.message.current.result,
          },
        },
      };
    }
  },
};

test("Broker can send list of tasks", async (done) => {
  const brokerInstance1 = await setup({
    ...currentConfig[0],
    ...brokerConfig,
  });

  const brokerInstance2 = await setup({
    ...currentConfig[1],
    ...brokerConfig,
  });

  const brokerInstance3 = await setup({
    ...currentConfig[2],
    ...brokerConfig,
  });

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHander(msg) {
    const objWork: any = msg;
    winston.debug("Worker receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message.current) {
      objWork.message.current.result = objWork.message.current.params[0] +
      objWork.message.current.params[1];
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {/*Do nothing*/});
    }
    return this.send(objWork);
  });

  const mockWorkerFn2 = jest.fn().mockImplementation(async function workerHander(msg) {
    const objWork: any = msg;
    winston.debug("Worker 2 receive works \n", objWork);
    /* sample do work */
    if (objWork && objWork.message.current) {
      /* Eventualy it should come from broker 1 to broker 2 and stop here */
      objWork.message.current.result = objWork.message.current.params[0] + objWork.message.current.params[1];
      expect(objWork.message.current.result).toBe(11);
      clientInst.deinit();
      workerInst.deinit();
      workerInst2.deinit();
      workerInst3.deinit();
      const p1 = await teardown(brokerInstance1);
      const p2 = await teardown(brokerInstance2);
      const p3 = await teardown(brokerInstance3);
      await p1;
      await p2;
      await p3;
      done();
    } else {
      /* backoff before request new one */
      await repeatIn(1000, 1000, () => {/* Do nothing */});
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
    onMessage: mockWorkerFn,
    port: currentConfig[1].backPort,
    socketType: currentConfig[1].clientType,
  });

  const workerInst3 = await initClient({
    onMessage: mockWorkerFn2,
    port: currentConfig[2].backPort,
    socketType: currentConfig[2].clientType,
  });

  const sentMsg = {
    message: {
      current: {
        params: [
          0, 1,
        ],
        result: 1,
        type: "task0",
      },
      dones: [],
      msgs: [
        {
          params: [
            1, 2,
          ],
          type: "task1",
        }, {
          params: [
            3, 4,
          ],
          type: "task2",
        }, {
          params: [
            5, 6,
          ],
          type: "task3",
        },
      ],
    },
  };
  clientInst.send(sentMsg);
  workerInst.send(null);
  workerInst2.send(null);
  workerInst3.send(null);
});

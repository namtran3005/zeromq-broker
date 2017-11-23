import * as BPromise from "bluebird";
import * as path from "path";
import * as winston from "winston";
import ClientAck from "../src/ClientAck";
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
    const { message: {
            current,
            msgs,
            dones,
          },
          idx,
    } = req || {
      idx: undefined,
      message: {
        current: {},
        dones: [],
        msgs: [],
      },
    };
    if (current && current.result) {
      dones.push(current);
    }
    return {
      current: msgs.shift(),
      dones,
      idx,
      msgs,
    };
  },
  onFrontResp: (resp, payload) => {
    return {
      idx: payload.idx,
      result: resp,
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

  const mockClientFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });

  const mockWorkerFn = jest.fn().mockImplementation(async function workerHandler(msg) {
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

  const clientInst = await new ClientAck({
    frontPort: 9999,
    queueUrl: `tcp://localhost:${currentConfig[0].frontPort}`,
    type: "dealer",
  }).init();

  const workerInst = await initClient({
    onMessage: mockWorkerFn,
    port: currentConfig[0].backPort,
    socketType: currentConfig[0].clientType,
  });
  const msg = {
    message: {
      current: {
        params: [
          0, 1,
        ],
        result: 1,
        type: "task1",
      },
      dones: [],
      msgs: [
        {
          params: [
            2, 3,
          ],
          type: "task2",
        }, {
          params: [
            4, 5,
          ],
          type: "task3",
        }, {
          params: [
            6, 7,
          ],
          type: "task4",
        },
      ],
    },
  };
  const arrPromise = [];
  for (let i = 0; i < 3; i++) {
    msg.message.current.type = i.toString();
    arrPromise.push(clientInst.send(msg).then((resp: any) => {
      expect(resp.dones[0].type).toBe(i.toString());
    }));
  }
  BPromise.all(arrPromise).then(async () => {
    clientInst.deinit();
    workerInst.deinit();
    await teardown(brokerInstance1);
    await teardown(brokerInstance2);
    done();
  });
  workerInst.send(null);
});

import * as path from "path";
import * as winston from "winston";
import {getRandomInt, initClient, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("An client can't create a task when the queue is full", async (done) => {
  const intMax = getRandomInt(1, 15);
  const intReject = getRandomInt(1, 5);
  const brokerInstance = await setup({
    ...currentConfig,
    ...{maxQueue: intMax},
  });
  let numReceived = 0;
  let numReject = 0;
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    if (msg === "rejected") {
      numReject += 1;
    } else if (msg === "received") {
      numReceived += 1;
    }
    return msg;
  });

  let arrClients = [];
  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients.push(initClient({
      onMessage: (msg) => mockFn(msg, i),
      port: currentConfig.frontPort,
      socketType: currentConfig.clientType,
    }));
  }
  arrClients = await Promise.all(arrClients);

  for (let i = 0; i < intMax + intReject; i += 1) {
    arrClients[i].send({
      params: [Math.random()],
      type: "task",
    });
  }

  setTimeout(() => {
    expect(mockFn).toHaveBeenCalledTimes(intMax + intReject);
    expect(numReceived).toBe(intMax);
    expect(intReject).toBe(numReject);
    for (let i = 0; i < intMax + intReject; i += 1) {
      arrClients[i].deinit();
    }
    teardown(brokerInstance).then(done);
  }, 3000);
});

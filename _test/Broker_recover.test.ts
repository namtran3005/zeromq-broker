import * as path from "path";
import winston from "winston";
import {initClient, repeatIn, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("A Broker instance after recover should get correct current numTask", async (done) => {
  const intMax = 100;
  const brokerInstance = await setup({
    ...currentConfig,
    ...{maxQueue: intMax},
  });

  let totalClient = 0;
  const mockFn = jest.fn().mockImplementation((msg, i) => {
    return msg;
  });
  const arrPromises = [];
  const arrClients = [];
  arrPromises.push(repeatIn(5000, 1000, () => brokerInstance.restart()));
  arrPromises.push(repeatIn(5000, 250, async () => {
    const numClient = 5;
    for (let i = 0; i < numClient; i += 1) {
      const numId = totalClient;
      totalClient += 1;
      const client = await initClient({
        onMessage: (msg) => mockFn(msg, numId),
        port: currentConfig.frontPort,
        socketType: currentConfig.clientType,
      });
      client.send({
        params: [Math.random()],
        type: "task",
      });
      arrClients.push(client);
    }
  }));
  Promise.all(arrPromises).then(async () => {
    expect(mockFn).toHaveBeenCalledTimes(totalClient);
    expect(brokerInstance.numTask).toBe(totalClient);
    for (let i = 0; i < totalClient; i += 1) {
      await arrClients[i].deinit();
    }
    teardown(brokerInstance).then(done);
  });
});

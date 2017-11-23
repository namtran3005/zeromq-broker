import MongoSMQ from "mongo-message";
import * as path from "path";
import * as winston from "winston";
import {setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

async function init() {
  const brokerInstance = await setup(currentConfig);
  expect(brokerInstance.queueInst).toBeInstanceOf(MongoSMQ);
  await teardown(brokerInstance);
}

test("Initiate new Broker instance", async () => {
  await init();
});

test("Initiate another Broker instance should success", async () => {
  await init();
});

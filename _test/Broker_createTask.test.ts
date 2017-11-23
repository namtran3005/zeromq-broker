import * as path from "path";
import * as winston from "winston";
import {initClient, setup, teardown} from "../utils";
import config from "./config";

winston.level = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

const currentFile = path.basename(__filename, ".test.ts");
const currentConfig = config[currentFile];

test("An client can create a task successfully", async (done) => {
  const brokerInstance = await setup(currentConfig);
  const mockFn = jest.fn().mockImplementation((msg) => {
    expect(msg).toBe("received");
    expect(mockFn).toHaveBeenCalledTimes(1);
    expect(brokerInstance.numTask).toBe(1);
    clientInst.deinit();
    teardown(brokerInstance).then(done);
  });

  const clientInst = await initClient({
    onMessage: mockFn,
    port: currentConfig.frontPort,
    socketType: currentConfig.clientType,
  });

  clientInst.send({
    params: [Math.random()],
    type: "task",
  });
});

test("An client can fails to create a task", async (done) => {
  const brokerInstance = await setup(currentConfig);
  brokerInstance.queueInst.createMessage = () => {
    throw new Error("error");
  };
  const mockFn = jest.fn().mockImplementation((msg) => {
    expect(brokerInstance.numTask).toBe(0);
    expect(mockFn).toHaveBeenCalledTimes(1);
    expect(msg).toBe("rejected");
    clientInst.deinit();
    teardown(brokerInstance).then(done);
  });

  const clientInst = await initClient({
    onMessage: mockFn,
    port: currentConfig.frontPort,
    socketType: currentConfig.clientType,
  });

  clientInst.send({
    params: [Math.random()],
    type: "task",
  });
});

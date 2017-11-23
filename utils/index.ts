import * as BPromise from "bluebird";
import Broker from "../src/Broker";
import Client from "../src/Client";

export const repeatIn = (ms: number, interval: number, cb: () => void) => {
  let countDown = ms;
  return new BPromise((resolve) => {
    const timerId = setInterval(async () => {
      if (countDown === 0) {
        clearInterval(timerId);
        resolve();
        return;
      }
      await cb();
      countDown -= interval;
    }, interval);
  });
};

export const sleep = (ms) => repeatIn(ms, ms, () => {/* Do nothing */});
/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 * Using Math.round() will give you a non-uniform distribution!
 */
export function getRandomInt(min, max) {
  return Math.floor(Math.random() * ((max - min) + 1)) + min;
}

export async function setup(options) {
  const opts = Object.assign({}, {
    backPort: 5552,
    frontPort: 5551,
    maxQueue: 10,
    queueName: "queue1",
  }, options);
  const fixtures = await (new Broker(opts)).initBroker();
  return fixtures;
}

export async function teardown(fixtures) {
  return fixtures.cleanQueue().then(() => fixtures.deInitBroker())
    .then(() => sleep(888));
}

export function initClient({hostname, port, socketType, onMessage}: {
  hostname?: string,
  port: number,
  socketType?: string,
  onMessage: (payload: any) => any,
}) {
  return new Client({
    onMessage,
    queueUrl: `tcp://${hostname || "localhost"}:${port}`,
    type: socketType,
  }).init();
}

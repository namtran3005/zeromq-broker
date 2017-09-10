/* @flow */
type MongoSMQ$options = {
    host?: string,
    db?: string,
    port?: number,
    options?: any,
    client?: ?string,
    ns?: string,
    visibility?: number,
};

declare module "mongo-message" {
  // Defines the type of a Path class within this `declare module` body, but 
  // does not export it. It can only be referenced by other things inside the
  // body of this `declare module`
  declare class MongoSMQ {
    constructor(options?: MongoSMQ$options): MongoSMQ;
    init(): Promise<MongoSMQ>;
    deinit(): Promise<Mongoose$Connection>;
    createMessage(payload: mixed): Object;

  }
  // Declares a named "concatPath" export which returns an instance of the
  // `Path` class (defined above)
  declare export default Class<MongoSMQ>;
}
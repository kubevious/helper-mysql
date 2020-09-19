export class HandledError extends Error {
  constructor(message: string) {
      super(message);
      Object.setPrototypeOf(this, HandledError.prototype);
      // this.name = this.constructor.name;
      // Error.captureStackTrace(this, this.constructor);
  }
}
declare module "event-emitter-es6" {
  class EventEmitter {
    on(event: string, handler: Function): void;
    off(event: string, handler: Function): void;
    emit(event: string, ...args: any[]): void;
  }

  export default EventEmitter;
}

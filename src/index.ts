import * as proto from "@zensum/event-store-proto";
import EventEmitter = require("event-emitter-es6");
const WebSocket =
  typeof window !== "undefined" ? (window as any).WebSocket : require("ws");

const CTL_UPDATE_DELAY = 100;

const { ControlPacket, Event: ProtoEvent } = proto.se.zensum.event_store_proto;
type IEvent = proto.se.zensum.event_store_proto.IEvent;
type IControlPacket = proto.se.zensum.event_store_proto.IControlPacket;
type IPublish = proto.se.zensum.event_store_proto.ControlPacket.IPublish;

interface Subscription {
  topic: Topic;
  keysToAdd: string[];
  keysToRemove: string[];
}

interface PendingSubscription {
  topic: Topic;
  key: Key;
}

interface PendingRewind {
  topic: Topic;
  keys: Key[];
  fromStart: boolean;
  n: number;
}

type Subscriptions = Record<Topic, Record<Key, boolean>>;

type Topic = string;

type Key = string;

const calcUpdates = (
  subs: PendingSubscription[],
  unsubs: PendingSubscription[]
) => {
  const topics = subs.reduce(
    (acc, { topic, key }) => {
      if (!acc[topic]) {
        acc[topic] = { topic, keysToAdd: [key], keysToRemove: [] };
      } else {
        acc[topic].keysToAdd.push(key);
      }
      return acc;
    },
    {} as Record<string, Subscription>
  );

  return unsubs.reduce((acc, { topic, key }) => {
    if (!topics[topic]) {
      acc[topic] = { topic, keysToAdd: [], keysToRemove: [key] };
    } else {
      acc[topic].keysToRemove.push(key);
    }
    return acc;
  }, topics);
};

class LatchedTimer {
  target: Function;
  interval: number;
  latch: boolean;

  constructor(target: Function, interval: number) {
    this.target = target;
    this.interval = interval;
    this.latch = false;
    this.triggered = this.triggered.bind(this);
  }

  triggered() {
    this.latch = false;
    this.target();
  }

  schedule() {
    if (this.latch) {
      return;
    }
    setTimeout(this.triggered, this.interval);
    this.latch = true;
  }
}

class EventStoreProtocol extends EventEmitter {
  socket: WebSocket;
  buffer: Uint8Array[];

  constructor(socket: WebSocket) {
    super();
    this.buffer = [];
    this.socket = socket;
    this.socket.binaryType = "arraybuffer";
    this.socket.addEventListener("open", () => {
      const buffered = this.buffer;
      this.buffer = [];
      buffered.forEach(bytes => this.socket.send(bytes));
      this.emit("open");
    });
    this.socket.addEventListener("message", e => {
      this.emit("message", ProtoEvent.decode(new Uint8Array(e.data)));
    });
  }

  send(data: IControlPacket) {
    const bytes = ControlPacket.encode(data).finish();
    if (this.socket.readyState != WebSocket.OPEN) {
      this.buffer.push(bytes);
    } else {
      this.socket.send(bytes);
    }
  }
}

class BatchManager extends EventEmitter {
  subscriptions: Subscriptions;
  pendingSubs: PendingSubscription[];
  pendingUnsubs: PendingSubscription[];
  pendingRewinds: PendingRewind[];
  pendingPublishes: IPublish[];
  timer: LatchedTimer;

  constructor() {
    super();
    this.subscriptions = {};
    this.pendingSubs = [];
    this.pendingUnsubs = [];
    this.pendingRewinds = [];
    this.pendingPublishes = [];
    this.timer = new LatchedTimer(this.flush.bind(this), CTL_UPDATE_DELAY);
  }

  publish(topic: Topic, key: Key, message: Uint8Array) {
    this.pendingPublishes.push({ topic, key, message });
    setTimeout(this.flush.bind(this), 0);
  }

  subscribe(topic: Topic, key: Key, subscriptionState: boolean) {
    const subscribed =
      this.subscriptions[topic] && this.subscriptions[topic][key];
    const targetList = subscriptionState
      ? this.pendingSubs
      : this.pendingUnsubs;
    if (subscribed !== subscriptionState) {
      targetList.push({ topic, key });
    }
    this.timer.schedule();
  }

  rewind(topic: Topic, keys: string[], fromStart: boolean, n: number) {
    this.pendingRewinds.push({ topic, keys, fromStart, n });
    this.timer.schedule();
  }

  setSubscription(state: boolean) {
    return (s: PendingSubscription) => {
      if (!this.subscriptions[s.topic]) {
        this.subscriptions[s.topic] = {};
      }
      this.subscriptions[s.topic][s.key] = state;
    };
  }

  flush() {
    const newSubs = calcUpdates(this.pendingSubs, this.pendingUnsubs);

    const pck: IControlPacket = ControlPacket.fromObject({
      subscriptions: Object.keys(newSubs).map(x => newSubs[x]),
      rewinds: this.pendingRewinds, // Dedup this?
      publishes: this.pendingPublishes
    });

    this.pendingSubs.forEach(this.setSubscription(true));
    this.pendingUnsubs.forEach(this.setSubscription(false));

    this.pendingRewinds = [];
    this.pendingUnsubs = [];
    this.pendingSubs = [];
    this.pendingPublishes = [];

    this.emit("flush", pck);
  }
}

type EventHandler = (data: Uint8Array) => void;

class EventDispatcher {
  dispatchTable: { [topic in Topic]: { [key in Key]: EventEmitter } };
  constructor() {
    this.dispatchTable = {};
  }
  getOrCreateEmitter(topic: Topic, key: Key) {
    const emitter = this.getEmitter(topic, key);
    if (emitter) {
      return emitter;
    } else {
      if (!this.dispatchTable[topic]) {
        this.dispatchTable[topic] = {};
      }
      this.dispatchTable[topic][key] = new EventEmitter();
      return this.dispatchTable[topic][key];
    }
  }
  getEmitter(topic: Topic, key: Key) {
    return this.dispatchTable[topic] && this.dispatchTable[topic][key];
  }
  addHandler(topic: Topic, key: Key, handler: EventHandler) {
    this.getOrCreateEmitter(topic, key).on("message", handler);
  }
  removeHandler(topic: Topic, key: Key, handler: EventHandler) {
    const node = this.getEmitter(topic, key);
    if (!node) {
      return;
    }
    node.off("message", handler);
  }
  incomingEvent(e: IEvent) {
    if (!e.topic || !e.key || !e.data) {
      return;
    }
    const emitter = this.getEmitter(e.topic, e.key);
    if (!emitter) {
      return;
    }
    e.data.forEach(data => emitter.emit("message", data));
  }
}

class Client {
  eventDispatcher: EventDispatcher;
  subMgr: BatchManager;
  rewind: (topic: Topic, keys: string[], fromStart: boolean, n: number) => void;

  constructor(url: string) {
    const socket = new WebSocket(url);
    this.eventDispatcher = new EventDispatcher();
    const protocol = new EventStoreProtocol(socket);
    protocol.on(
      "message",
      this.eventDispatcher.incomingEvent.bind(this.eventDispatcher)
    );
    this.subMgr = new BatchManager();
    this.subMgr.on("flush", protocol.send.bind(protocol));
    protocol.on("open", this.subMgr.flush.bind(this.subMgr));
    this.rewind = this.subMgr.rewind.bind(this.subMgr);
  }

  subscribe(topic: Topic, key: Key, handler: EventHandler) {
    this.eventDispatcher.addHandler(topic, key, handler);
    this.subMgr.subscribe(topic, key, true);
  }

  unsubscribe(topic: Topic, key: Key, handler: EventHandler) {
    this.eventDispatcher.removeHandler(topic, key, handler);
    this.subMgr.subscribe(topic, key, false);
  }

  publish(topic: Topic, key: Key, message: Uint8Array) {
    this.subMgr.publish(topic, key, message);
  }
}

module.exports = Client;

import * as proto from "@zensum/event-store-proto";
import * as mitt from 'mitt';
import { Emitter } from "mitt";

const WebSocket =
  typeof window !== "undefined" ? (window as any).WebSocket : require("ws");

const DEBUG_TAG = "event-store-client";

const CTL_UPDATE_DELAY = 100;
const INITIAL_RECONNECT_TIMEOUT = 500;

const { ControlPacket, Event: ProtoEvent } = proto.se.zensum.event_store_proto;
export type IEvent = proto.se.zensum.event_store_proto.IEvent;
export type IControlPacket = proto.se.zensum.event_store_proto.IControlPacket;
export type IPublish = proto.se.zensum.event_store_proto.ControlPacket.IPublish;

interface Subscription {
  topic: Topic;
  keysToAdd: string[];
  keysToRemove: string[];
}

export interface PendingSubscription {
  topic: Topic;
  key: Key;
}

export interface PendingRewind {
  topic: Topic;
  keys: Key[];
  fromStart: boolean;
  n: number;
}

export type Subscriptions = Record<Topic, Record<Key, boolean>>;

export type Topic = string;

export type Key = string;

const debug = (...args: any[]) => console.debug(DEBUG_TAG, ...args);

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

export class LatchedTimer {
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

export class EventStoreProtocol {
  socket: WebSocket;
  buffer: Uint8Array[];
  ev: Emitter;

  constructor(socket: WebSocket) {
    this.buffer = [];
    this.socket = socket;
    this.socket.binaryType = "arraybuffer";
    this.ev = new mitt();
    this.socket.addEventListener("open", () => {
      const buffered = this.buffer;
      this.buffer = [];
      buffered.forEach(bytes => this.socket.send(bytes));
      this.ev.emit("open", {});
    });
    this.socket.addEventListener("error", this.ev.emit.bind(null, "error"));
    this.socket.addEventListener("close", this.ev.emit.bind(null, "close"));
    this.socket.addEventListener("message", e => {
      this.ev.emit("message", ProtoEvent.decode(new Uint8Array(e.data)));
    });
  }

  send(data: IControlPacket) {
    const bytes = ControlPacket.encode(data).finish();
    debug("Trying to send data", data, bytes);
    if (this.socket.readyState != WebSocket.OPEN) {
      debug("WebSocket not open. Adding data to buffer", data, bytes);
      this.buffer.push(bytes);
    } else {
      debug("Sending data", data, bytes);
      this.socket.send(bytes);
    }
  }
}

export class BatchManager {
  subscriptions: Subscriptions;
  pendingSubs: PendingSubscription[];
  pendingUnsubs: PendingSubscription[];
  pendingRewinds: PendingRewind[];
  pendingPublishes: IPublish[];
  timer: LatchedTimer;
  ev: Emitter
  constructor() {
    this.ev = new mitt();
    this.subscriptions = {};
    this.pendingSubs = [];
    this.pendingUnsubs = [];
    this.pendingRewinds = [];
    this.pendingPublishes = [];
    this.timer = new LatchedTimer(this.flush.bind(this), CTL_UPDATE_DELAY);
  }

  publish(topic: Topic, key: Key, message: Uint8Array) {
    debug(
      "Received message on topic '" + topic + "' with key '" + key + "'",
      message
    );
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

    debug("Subscribing", this.pendingSubs);
    this.pendingSubs.forEach(this.setSubscription(true));

    debug("Unsubscribing", this.pendingUnsubs);
    this.pendingUnsubs.forEach(this.setSubscription(false));

    this.pendingRewinds = [];
    this.pendingUnsubs = [];
    this.pendingSubs = [];
    this.pendingPublishes = [];

    debug("Flushing batch", pck);
    this.ev.emit("flush", pck);
  }
}

export type EventHandler = (data: Uint8Array) => void;

export class EventDispatcher {
  dispatchTable: { [topic in Topic]: { [key in Key]: Emitter } };
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
      this.dispatchTable[topic][key] = new mitt();
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

export default class Client {
  eventDispatcher: EventDispatcher;
  protocol: EventStoreProtocol;
  subMgr: BatchManager;
  rewind: (topic: Topic, keys: string[], fromStart: boolean, n: number) => void;
  url: string;
  httpUrl: string;
  reconnectTimeout: number;

  constructor(url: string) {
    this.url = url;
    this.httpUrl = this.url.replace(/^ws/, "http").replace(/realtime\/?$/, "");
    this.eventDispatcher = new EventDispatcher();
    this.subMgr = new BatchManager();
    this.initializeProtocol();
    this.rewind = this.subMgr.rewind.bind(this.subMgr);
    this.reconnectTimeout = INITIAL_RECONNECT_TIMEOUT;
  }

  initializeProtocol() {
    const socket = new WebSocket(this.url);
    this.protocol = new EventStoreProtocol(socket);
    this.protocol.ev.on("message", msg => {
      this.eventDispatcher.incomingEvent(msg);
      this.reconnectTimeout = INITIAL_RECONNECT_TIMEOUT;
    });
    this.subMgr.ev.on("flush", this.protocol.send.bind(this.protocol));
    this.protocol.ev.on("open", this.subMgr.flush.bind(this.subMgr));
    this.protocol.ev.on("close", () => {
      window.setTimeout(
        this.initializeProtocol.bind(this),
        this.reconnectTimeout + Math.random() * 250
      );
      this.reconnectTimeout = this.reconnectTimeout * 2;
    });
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

  publishReliably(
    topic: Topic,
    key: Key,
    message: Uint8Array
  ): Promise<string> {
    const url = `${this.httpUrl}/publish?topic=${topic}&key=${key}`;
    return window
      .fetch(url, { method: "POST", body: message })
      .then(x => x.text());
  }
}

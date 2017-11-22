const WebSocket = require('ws');
const proto = require('@zensum/event-store-proto')
const EventEmitter = require('event-emitter-es6')

const {ControlPacket, Event} = proto.se.zensum.event_store_proto

const CTL_UPDATE_DELAY = 100

const eventBatchToEvents = e => 
      e.data.map(x => ({ topic: e.topic, key: e.key, data: x }))

const calcUpdates = (initial, subs, unsubs) => {
  const topics = subs
        .reduce((acc, {topic, key}) => {
          if (!acc[topic]) {
            acc[topic] = { topic, keysToAdd: [key], keysToRemove: [] }
          } else {
            acc[topic].keysToAdd.push(key)
          }
          return acc
        }, initial)

  return unsubs
    .reduce((acc, {topic, key}) => {
      if (!topics[topic]) {
        acc[topic] = { topic, keysToRemove: [key] }
      } else {
        acc[topic].keysToRemove.push(key)
      }
      return acc
    }, topics)
}

class LatchedTimer {
  constructor(target, interval) {
    this.target = target
    this.interval = interval
    this.latch = false
    this.triggered = this.triggered.bind(this)
  }

  triggered() {
    this.latch = false
    this.target()
  }

  schedule() {
    if (this.latch) {
      return 
    }
    setTimeout(this.triggered, this.interval)
    this.latch = true
  }
}

class EventStoreProtocol extends EventEmitter {
  constructor(socket) {
    super()
    this.buffer = []
    this.socket = socket
    this.socket.binaryType = "arraybuffer";
    this.socket.addEventListener('open', () => {
      const buffered = this.buffer
      this.buffer = []
      buffered.forEach(x => this.socket.send(x))
      this.emit('open')
    })
    this.socket.addEventListener('message', e => {
      this.emit('message', Event.decode(new Uint8Array(e.data)))
    })
  }

  send(data) {
    const bytes = ControlPacket.encode(data).finish()
    if (this.socket.readyState != WebSocket.OPEN) {
      this.buffer.push(bytes)
    } else {
      this.socket.send(bytes)
    }
  }
}

class BatchManager extends EventEmitter {
  constructor() {
    super()
    this.subscriptions = {}
    this.pendingSubs = []
    this.pendingUnsubs = []
    this.pendingRewinds = []
    this.timer = new LatchedTimer(this.flush.bind(this), CTL_UPDATE_DELAY)
  }

  subscribe(topic, key, subscriptionState) {
    const subscribed = this.subscriptions[topic] && this.subscriptions[topic][key]
    const targetList = subscriptionState ? this.pendingSubs : this.pendingUnsubs
    if (subscribed !== subscriptionState) {
      targetList.push({topic, key})
    }
    this.timer.schedule()
  }
  
  rewind(topic, keys, fromStart, n) {
    this.pendingRewinds.push({topic, keys, fromStart, n})
    this.timer.schedule()
  }

  flush() {
    const newSubs = calcUpdates({}, this.pendingSubs, this.pendingUnsubs)
        
    const pck = ControlPacket.fromObject({
      subscriptions: Object.keys(newSubs).map(x => newSubs[x]),
      rewinds: this.pendingRewinds, // Dedup this?
    })

    this.subscriptions = calcUpdates(this.subscriptions, this.pendingSubs, this.pendingUnsubs)
    this.emit('flush', pck)
  }
  
}

class EventDispatcher {
  constructor() {
    this.dispatchTable = {}
  }
  getOrCreateEmitter(topic, key) {
    const emitter = this.getEmitter(topic, key)
    if (emitter) {
      return emitter
    } else {
      if (!this.dispatchTable[topic]) {
        this.dispatchTable[topic] = {}
      }
      this.dispatchTable[topic][key] = new EventEmitter()
      return this.dispatchTable[topic][key]
    }
  }
  getEmitter(topic, key) {
    return this.dispatchTable[topic] && this.dispatchTable[topic][key]
  }
  addHandler(topic, key, handler) {
    this.getOrCreateEmitter(topic, key).on('message', handler)
  }
  removeHandler(topic, key, handler) {
    const node = this.getEmitter(topic, key)
    if (!node) {
      return
    }
    node.off('message', handler)
  }
  incomingEvent(e) {
    const emitter = this.getEmitter(e.topic, e.key)
    if (!emitter) {
      return
    }
    e.data.forEach(data => emitter.emit('message', data))
  }
}

class Client {
  constructor(url) {
    const socket = new WebSocket(url)
    this.eventDispatcher = new EventDispatcher()
    const protocol = new EventStoreProtocol(socket)
    protocol.on('message', this.eventDispatcher.incomingEvent.bind(this.eventDispatcher))
    this.subMgr = new BatchManager()
    this.subMgr.on('flush', protocol.send.bind(this.protocol))
    protocol.on('open', this.subMgr.flush.bind(this.subMgr))
    this.rewind = this.subMgr.rewind.bind(this.subMgr)
  }

  subscribe(topic, key, handler) {
    this.eventDispatcher.addHandler(topic, key, handler)
    this.subMgr.subscribe(topic, key, true)
  }

  unsubscribe(topic, key, handler) {
    this.eventDispatcher.removeHandler(topic, key, handler)
    this.subMgr.unsubscribe(topic, key, false)
  }
}

const cli = new Client()

cli.subscribe("sms", "AAABXsUq9uIKAAMzAAAAAA", x => console.log(x.length))
cli.rewind("sms", ["AAABXsUq9uIKAAMzAAAAAA"], false, 100)

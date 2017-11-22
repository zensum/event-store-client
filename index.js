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
    this.socket = socket
    this.socket.binaryType = "arraybuffer";
    this.connected = false
    this.socket.addEventListener('open', () => {
      this.connected = true
      this.emit('open')
    })
    this.socket.addEventListener('message', e => {
      this.emit('message', Event.decode(new Uint8Array(e.data)))
    })
  }

  isAvaliable() {
    return this.connected
  }

  sendControlPacket(data) {
    const writer = ControlPacket.encode(data)
    this.socket.send(writer.finish())
  }
}

class BatchManager extends EventEmitter {
  constructor(isAvaliable) {
    super()
    this.isAvaliable = isAvaliable
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
    if (!this.isAvaliable()) {
      this.timer.schedule()
      return
    }

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
  addHandler(topic, key, handler) {
    if(!this.dispatchTable[topic]) {
      this.dispatchTable[topic] = {}
    }
    if (!this.dispatchTable[topic][key]) {
      this.dispatchTable[topic][key] = []
    }
    this.dispatchTable[topic][key].push(handler)
  }
  removeHandler(topic, key, handler) {
    if (!this.dispatchTable[topic] || !this.dispatchTable[topic][key]) {
      return
    }
    this.dispatchTable[topic][key] = this.dispatchTable[topic][key].filter(x => x != handler)

    if(this.dispatchTable[topic][key].length < 1) {
      delete this.dispatchTable[topic][key]
    }
    if (Object.keys(this.dispatchTable[topic]).length === 0) {
      delete this.dispatchTable[topic]
    }
  }
  incomingEvent(e) {
    const node = this.dispatchTable[e.topic] && this.dispatchTable[e.topic][e.key]
    if (!node) {
      return
    }
    e.data.forEach(data => {
      (node || []).map(fn => fn(data))
    })
  }
}

const mkSocket = () => new WebSocket('ws://event-store.5z.fyi/realtime')

class Client {
  constructor() {
    this.eventDispatcher = new EventDispatcher()
    this.protocol = new EventStoreProtocol(mkSocket())
    this.protocol.on('message', this.eventDispatcher.incomingEvent.bind(this.eventDispatcher))
    this.subMgr = new BatchManager(this.protocol.isAvaliable.bind(this.protocol))
    this.subMgr.on('flush', this.protocol.sendControlPacket.bind(this.protocol))
                                  
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

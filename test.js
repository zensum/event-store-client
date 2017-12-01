const EventStoreClient = require("./dist/index.js");

const EVENT_STORE_WEBSOCKET_URL = "ws://event-store.5z.fyi/realtime";
const TEST_ID = "AAABXsUq9uIKAAMzAAAAAA";

const client = new EventStoreClient(EVENT_STORE_WEBSOCKET_URL);
client.subscribe("sms", TEST_ID, x => console.log(x.length));
client.rewind("sms", [TEST_ID], false, 100);

import { RealtimeWorkerCore } from "./realtime-core";
import type { WorkerInbound, WorkerOutbound } from "../types";

const core = new RealtimeWorkerCore((message: WorkerOutbound) => {
  self.postMessage(message);
});

self.onmessage = (event: MessageEvent<WorkerInbound>) => {
  void core.handleMessage(event.data);
};

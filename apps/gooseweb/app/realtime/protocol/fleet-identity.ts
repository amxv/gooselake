import { ProtocolDecodeError } from "./protocol-error";

type FleetIdentity = {
  readonly sourceId: string;
  readonly sessionId: string;
  readonly rowId: string;
};

export function fleetEntityKey(row: FleetIdentity): string {
  if (!row.sourceId || !row.sessionId ||
    (row.rowId !== row.sessionId && row.rowId !== `${row.sourceId}:${row.sessionId}`)) {
    throw new ProtocolDecodeError("fleet row identity disagrees with its source session");
  }
  return `${encodeURIComponent(row.sourceId)}::${encodeURIComponent(row.sessionId)}`;
}

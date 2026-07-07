const defaultGoosetowerHttpUrl = "http://127.0.0.1:8090";

export function goosetowerHttpTarget(
  realtimeUrl: string,
  explicitHttpUrl?: string
): string {
  if (explicitHttpUrl?.trim()) {
    return normalizeHttpTarget(explicitHttpUrl);
  }

  return httpTargetFromRealtimeUrl(realtimeUrl);
}

export function httpTargetFromRealtimeUrl(realtimeUrl: string): string {
  try {
    const url = new URL(realtimeUrl);
    if (url.protocol === "ws:") {
      url.protocol = "http:";
    } else if (url.protocol === "wss:") {
      url.protocol = "https:";
    } else if (url.protocol !== "http:" && url.protocol !== "https:") {
      return defaultGoosetowerHttpUrl;
    }
    url.pathname = "/";
    url.search = "";
    url.hash = "";
    return normalizeHttpTarget(url.toString());
  } catch {
    return defaultGoosetowerHttpUrl;
  }
}

function normalizeHttpTarget(value: string): string {
  const url = new URL(value);
  url.pathname = "/";
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/$/, "");
}

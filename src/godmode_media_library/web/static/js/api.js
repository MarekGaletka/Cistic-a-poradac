/* GOD MODE Media Library — API helpers */

function _getToken() {
  const meta = document.querySelector('meta[name="gml-api-token"]');
  return meta ? meta.content : "";
}

function _authHeaders() {
  const token = _getToken();
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}

async function _parseErrorBody(res) {
  try {
    const data = await res.json();
    return data.detail || data.message || data.error || JSON.stringify(data);
  } catch {
    try { return await res.text(); } catch { return ""; }
  }
}

export async function api(path) {
  const res = await fetch(`/api${path}`, { headers: _authHeaders() });
  if (!res.ok) {
    const detail = await _parseErrorBody(res);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export async function apiPost(path, body = null) {
  const opts = { method: "POST", headers: { ..._authHeaders() } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`/api${path}`, opts);
  if (!res.ok) {
    const detail = await _parseErrorBody(res);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export async function apiPut(path, body = null) {
  const opts = { method: "PUT", headers: { ..._authHeaders() } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`/api${path}`, opts);
  if (!res.ok) {
    const detail = await _parseErrorBody(res);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export async function apiDelete(path, body = null) {
  const opts = { method: "DELETE", headers: { ..._authHeaders() } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`/api${path}`, opts);
  if (!res.ok) {
    const detail = await _parseErrorBody(res);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

/** Get the API token for WebSocket connections */
export function getWsToken() {
  return _getToken();
}

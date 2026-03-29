/* GOD MODE Media Library — API helpers */

async function _parseErrorBody(res) {
  try {
    const data = await res.json();
    return data.detail || data.message || data.error || JSON.stringify(data);
  } catch {
    try { return await res.text(); } catch { return ""; }
  }
}

export async function api(path) {
  const res = await fetch(`/api${path}`);
  if (!res.ok) {
    const detail = await _parseErrorBody(res);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export async function apiPost(path, body = null) {
  const opts = { method: "POST" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
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
  const opts = { method: "PUT" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
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
  const opts = { method: "DELETE" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
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

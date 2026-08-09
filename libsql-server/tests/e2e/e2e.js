#!/usr/bin/env node

const assert = require("node:assert/strict");
const { after, before, describe, it } = require("node:test");
const fs = require("node:fs");
const http = require("node:http");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const { createClient } = require("@libsql/client");
const { createClient: createWsClient } = require("@libsql/client/ws");

const BASE_URL = process.argv.find(argument => argument.startsWith("http")) || "http://localhost:3000";
const ADMIN_URL = process.argv.includes("--admin-url")
  ? process.argv[process.argv.indexOf("--admin-url") + 1]
  : BASE_URL;
const ADMIN_KEY = process.argv.includes("--admin-key")
  ? process.argv[process.argv.indexOf("--admin-key") + 1]
  : "miniturso-admin-key-change-me";

let dbId;
let token;
let replicaFile;

async function fetchApi(method, requestPath, body) {
  const response = await fetch(`${ADMIN_URL}${requestPath}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${ADMIN_KEY}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const responseBody = await response.text();
  if (!response.ok) throw new Error(`${response.status}: ${responseBody}`);
  return responseBody ? JSON.parse(responseBody) : null;
}

async function sqlQuery(authToken, statements) {
  const response = await fetch(`${BASE_URL}/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${authToken}`,
    },
    body: JSON.stringify({ statements: Array.isArray(statements) ? statements : [statements] }),
  });

  const responseBody = await response.text();
  if (!response.ok) throw new Error(`${response.status}: ${responseBody}`);
  return JSON.parse(responseBody);
}

function rawHttpRequest(authToken, body) {
  return new Promise((resolve, reject) => {
    const requestModule = new URL(BASE_URL).protocol === "https:" ? https : http;
    const request = requestModule.request(BASE_URL, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${authToken}`,
        "Content-Length": Buffer.byteLength(body),
      },
    }, response => {
      let responseBody = "";
      response.setEncoding("utf8");
      response.on("data", chunk => { responseBody += chunk; });
      response.on("end", () => {
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`${response.statusCode}: ${responseBody}`));
          return;
        }
        resolve(JSON.parse(responseBody));
      });
    });

    request.on("error", reject);
    request.end(body);
  });
}

before(async () => {
  const response = await fetch(`${BASE_URL}/health`);
  assert.equal(response.ok, true, "MiniTurso health check");

  const database = await fetchApi("POST", "/api/databases", {
    id: `e2e${Date.now().toString(36)}`,
  });
  dbId = database.id;
  token = database.token;
});

after(async () => {
  if (replicaFile && fs.existsSync(replicaFile)) fs.unlinkSync(replicaFile);
  if (dbId) await fetchApi("DELETE", `/api/databases/${dbId}`);
});

describe("MiniTurso E2E", { concurrency: false }, () => {
  it("creates and lists databases", async () => {
    const response = await fetchApi("GET", "/api/databases");
    assert.ok(response.databases.some(database => database.id === dbId));
  });

  it("returns the same access token until it is regenerated", async () => {
    const first = await fetchApi("GET", `/api/databases/${dbId}/token`);
    assert.equal(first.token, token);

    const again = await fetchApi("GET", `/api/databases/${dbId}/token`);
    assert.equal(again.token, first.token);
  });

  it("returns the regenerated token after rotation", async () => {
    const database = await fetchApi("POST", "/api/databases", {
      id: `e2etoken${Date.now().toString(36)}`,
    });

    try {
      const before = await fetchApi("GET", `/api/databases/${database.id}/token`);
      assert.equal(before.token, database.token);

      const rotated = await fetchApi("POST", `/api/databases/${database.id}/token`);
      assert.notEqual(rotated.token, database.token);

      const afterRotate = await fetchApi("GET", `/api/databases/${database.id}/token`);
      assert.equal(afterRotate.token, rotated.token);
    } finally {
      await fetchApi("DELETE", `/api/databases/${database.id}`);
    }
  });

  it("returns 404 when fetching the token of an unknown database", async () => {
    const response = await fetch(`${ADMIN_URL}/api/databases/does-not-exist/token`, {
      headers: { "Authorization": `Bearer ${ADMIN_KEY}` },
    });
    assert.equal(response.status, 404);
  });

  it("reports the deployed image version", async () => {
    const response = await fetch(`${BASE_URL}/version`);
    assert.equal(response.ok, true);
    const body = await response.json();
    assert.equal(typeof body.version, "string");
    assert.ok(body.version.length > 0);
  });

  it("executes SQL through the HTTP pipeline", async () => {
    await sqlQuery(token, "CREATE TABLE e2e_test (id INTEGER PRIMARY KEY, msg TEXT)");
    await sqlQuery(token, [
      "INSERT INTO e2e_test VALUES (1, 'hello http')",
      "INSERT INTO e2e_test VALUES (2, 'from pipeline')",
    ]);

    const response = await sqlQuery(token, "SELECT * FROM e2e_test ORDER BY id");
    assert.deepEqual(response[0].results.rows, [
      [1, "hello http"],
      [2, "from pipeline"],
    ]);
  });

  it("executes SQL without an application/json request header", async () => {
    const response = await rawHttpRequest(token, JSON.stringify({
      statements: ["SELECT id, msg FROM e2e_test WHERE id = 1"],
    }));

    assert.deepEqual(response[0].results.rows, [[1, "hello http"]]);
  });

  it("executes SQL through Hrana", async () => {
    const client = createClient({ url: BASE_URL, authToken: token });
    await client.execute("INSERT INTO e2e_test VALUES (3, 'hello hrana')");

    const response = await client.execute("SELECT msg FROM e2e_test ORDER BY id");
    assert.deepEqual(response.rows.map(row => row.msg), [
      "hello http",
      "from pipeline",
      "hello hrana",
    ]);

    const batch = await client.batch([
      "INSERT INTO e2e_test VALUES (4, 'batch1')",
      "INSERT INTO e2e_test VALUES (5, 'batch2')",
      "SELECT count(*) AS cnt FROM e2e_test",
    ]);
    assert.ok(batch[2].rows[0].cnt >= 5);
  });

  it("executes SQL through the admin Hrana pipeline against any namespace", async () => {
    const response = await fetch(`${ADMIN_URL}/admin/v3/pipeline?ns=${dbId}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${ADMIN_KEY}`,
      },
      body: JSON.stringify({
        requests: [
          { type: "execute", stmt: { sql: "CREATE TABLE IF NOT EXISTS admin_pipeline (id INTEGER PRIMARY KEY, v TEXT)" } },
          { type: "execute", stmt: { sql: "INSERT INTO admin_pipeline (v) VALUES ('via admin pipeline')" } },
        ],
      }),
    });
    assert.equal(response.ok, true, `admin pipeline status ${response.status}`);
    const body = await response.json();
    assert.ok(Array.isArray(body.results));
    assert.equal(body.results.length, 2);

    const check = await sqlQuery(token, "SELECT v FROM admin_pipeline");
    assert.deepEqual(check[0].results.rows, [["via admin pipeline"]]);
  });

  it("rejects admin Hrana pipeline with a bad admin key", async () => {
    const response = await fetch(`${ADMIN_URL}/admin/v3/pipeline?ns=${dbId}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer wrong-admin-key",
      },
      body: JSON.stringify({ requests: [] }),
    });
    assert.equal(response.status, 401);
  });

  it.skip("executes SQL through Hrana over WebSocket (MiniTurso does not expose WebSocket)", async () => {
    const client = createWsClient({
      url: BASE_URL.replace(/^http/, "ws"),
      authToken: token,
    });

    try {
      assert.equal(client.protocol, "ws");
      await client.execute("INSERT INTO e2e_test VALUES (7, 'hello websocket')");

      const response = await client.execute("SELECT msg FROM e2e_test WHERE id = 7");
      assert.deepEqual(response.rows.map(row => row.msg), ["hello websocket"]);
    } finally {
      client.close();
    }
  });

  it("syncs an embedded replica", async () => {
    replicaFile = path.join(os.tmpdir(), `miniturso-replica-${dbId}.db`);
    const replica = createClient({
      url: `file:${replicaFile}`,
      syncUrl: BASE_URL,
      authToken: token,
    });

    await replica.sync();
    const response = await replica.execute("SELECT count(*) AS cnt FROM e2e_test");
    assert.ok(response.rows[0].cnt >= 5);
  });

  it("syncs writes from the embedded replica back to primary", async () => {
    const replica = createClient({
      url: `file:${replicaFile}`,
      syncUrl: BASE_URL,
      authToken: token,
    });

    await replica.execute("INSERT INTO e2e_test VALUES (6, 'from replica')");
    await replica.sync();

    const client = createClient({ url: BASE_URL, authToken: token });
    const response = await client.execute("SELECT count(*) AS cnt FROM e2e_test");
    assert.ok(response.rows[0].cnt >= 6);
  });
});

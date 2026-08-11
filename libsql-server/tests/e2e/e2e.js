#!/usr/bin/env node

const assert = require("node:assert/strict");
const { after, before, describe, it } = require("node:test");
const crypto = require("node:crypto");
const fs = require("node:fs");
const http = require("node:http");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const { createClient } = require("@libsql/client");

const BASE_URL = process.argv.find(argument => argument.startsWith("http")) || "http://localhost:3000";
const ADMIN_URL = process.argv.includes("--admin-url")
  ? process.argv[process.argv.indexOf("--admin-url") + 1]
  : BASE_URL;
const ADMIN_KEY = process.argv.includes("--admin-key")
  ? process.argv[process.argv.indexOf("--admin-key") + 1]
  : "admin-key-change-me";
const JWT_SECRET = process.argv.includes("--jwt-secret")
  ? process.argv[process.argv.indexOf("--jwt-secret") + 1]
  : process.env.SQLD_AUTH_JWT_SECRET || "";

if (!JWT_SECRET) {
  throw new Error("SQLD_AUTH_JWT_SECRET not configured; cannot mint test access tokens");
}

/** Mint a backend-style HS256 access token (same claims as the platform backend). */
function mintAccessToken(userID, ttlSeconds = 3600) {
  const enc = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64url");
  const header = enc({ alg: "HS256", typ: "JWT" });
  const payload = enc({
    jti: crypto.randomBytes(8).toString("hex"),
    role: "user",
    userID,
    exp: Math.floor(Date.now() / 1000) + ttlSeconds,
  });
  const sig = crypto
    .createHmac("sha256", JWT_SECRET)
    .update(`${header}.${payload}`)
    .digest("base64url");
  return `${header}.${payload}.${sig}`;
}

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
  token = mintAccessToken(dbId);
});

after(async () => {
  if (replicaFile && fs.existsSync(replicaFile)) fs.unlinkSync(replicaFile);
  if (dbId) await fetchApi("DELETE", `/api/databases/${dbId}`);
});

describe("Admin API E2E", { concurrency: false }, () => {
  it("creates and lists databases", async () => {
    const response = await fetchApi("GET", "/api/databases");
    assert.ok(response.databases.some(database => database.id === dbId));
  });

  it("reports the deployed image version", async () => {
    const response = await fetch(`${BASE_URL}/version`);
    assert.equal(response.ok, true);
    const body = await response.json();
    assert.equal(typeof body.version, "string");
    assert.ok(body.version.length > 0);
  });

  it("reads its own namespace with a backend access token", async () => {
    const response = await sqlQuery(token, "SELECT 1 AS one");
    assert.deepEqual(response[0].results.rows, [[1]]);
  });

  it("rejects writes on the public port (read-only by design)", async () => {
    const response = await fetch(`${BASE_URL}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ statements: ["CREATE TABLE nope (id INTEGER)"] }),
    });
    assert.equal(response.status, 403);
  });

  it("rejects writes through Hrana on the public port", async () => {
    const client = createClient({ url: BASE_URL, authToken: token });
    await assert.rejects(
      () => client.execute("CREATE TABLE nope_hrana (id INTEGER)"),
      /403|not allowed|writes are not allowed/,
    );
  });

  it("rejects an invalid access token", async () => {
    const response = await fetch(`${BASE_URL}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer not-a-real-token",
      },
      body: JSON.stringify({ statements: ["SELECT 1"] }),
    });
    // 400: namespace resolution fails before auth (no claims in the token)
    assert.ok([400, 401].includes(response.status), `status ${response.status}`);
  });

  it("rejects a token for another namespace", async () => {
    const otherToken = mintAccessToken("someone-else");
    const response = await fetch(`${BASE_URL}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${otherToken}`,
      },
      body: JSON.stringify({ statements: ["SELECT 1"] }),
    });
    // 404: the claimed namespace does not exist on this server
    assert.ok([403, 404].includes(response.status), `status ${response.status}`);
  });

  it("executes SQL without an application/json request header", async () => {
    const response = await rawHttpRequest(token, JSON.stringify({
      statements: ["SELECT 42 AS answer"],
    }));

    assert.deepEqual(response[0].results.rows, [[42]]);
  });

  it("seeds a table through the admin pipeline, then reads it on the public port", async () => {
    const seed = await fetch(`${ADMIN_URL}/v3/pipeline`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${ADMIN_KEY}`,
        "x-namespace": dbId,
      },
      body: JSON.stringify({
        requests: [
          { type: "execute", stmt: { sql: "CREATE TABLE IF NOT EXISTS e2e_test (id INTEGER PRIMARY KEY, msg TEXT)" } },
          { type: "execute", stmt: { sql: "INSERT INTO e2e_test VALUES (1, 'hello http')" } },
          { type: "execute", stmt: { sql: "INSERT INTO e2e_test VALUES (2, 'from pipeline')" } },
        ],
      }),
    });
    assert.equal(seed.ok, true, `admin seed status ${seed.status}`);

    const response = await sqlQuery(token, "SELECT * FROM e2e_test ORDER BY id");
    assert.deepEqual(response[0].results.rows, [
      [1, "hello http"],
      [2, "from pipeline"],
    ]);
  });

  it("executes SQL through Hrana", async () => {
    const client = createClient({ url: BASE_URL, authToken: token });
    const response = await client.execute("SELECT msg FROM e2e_test ORDER BY id");
    assert.deepEqual(response.rows.map(row => row.msg), [
      "hello http",
      "from pipeline",
    ]);
  });

  it("executes SQL through the admin-port Hrana v2 pipeline (the @libsql/client wire path)", async () => {
    const response = await fetch(`${ADMIN_URL}/v2/pipeline`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${ADMIN_KEY}`,
        "x-namespace": dbId,
      },
      body: JSON.stringify({
        requests: [
          { type: "execute", stmt: { sql: "INSERT INTO e2e_test (id, msg) VALUES (3, 'via v2 pipeline')" } },
        ],
      }),
    });
    assert.equal(response.ok, true, `admin v2 pipeline status ${response.status}`);
    const body = await response.json();
    assert.ok(Array.isArray(body.results));
    assert.equal(body.results.length, 1);

    const check = await sqlQuery(token, "SELECT msg FROM e2e_test WHERE id = 3");
    assert.deepEqual(check[0].results.rows, [["via v2 pipeline"]]);
  });

  it("rejects the admin pipeline with a bad admin key", async () => {
    const response = await fetch(`${ADMIN_URL}/v3/pipeline`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer wrong-admin-key",
        "x-namespace": dbId,
      },
      body: JSON.stringify({ requests: [] }),
    });
    assert.equal(response.status, 401);
  });

  it("rejects the admin pipeline without an x-namespace header", async () => {
    const response = await fetch(`${ADMIN_URL}/v3/pipeline`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${ADMIN_KEY}`,
      },
      body: JSON.stringify({ requests: [] }),
    });
    assert.equal(response.status, 400);
  });

  it("syncs an embedded replica (reads)", async () => {
    replicaFile = path.join(os.tmpdir(), `admin-api-replica-${dbId}.db`);
    const replica = createClient({
      url: `file:${replicaFile}`,
      syncUrl: BASE_URL,
      authToken: token,
    });

    await replica.sync();
    const response = await replica.execute("SELECT count(*) AS cnt FROM e2e_test");
    assert.ok(response.rows[0].cnt >= 3);
  });
});

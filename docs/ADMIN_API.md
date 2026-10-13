# Libsql-server admin API documentation

This document describes the admin API endpoints.

The admin API is used to manage namespaces on a `sqld` instance. Namespaces are isolated database within a same sqld instance.

The admin API is served on the port of the user API (`--http-listen-addr`). It is enabled by giving `sqld` a key, that every request to the admin API must carry as a bearer token:

- `--admin-key <key>` (or the `ADMIN_KEY` environment variable): the key of the admin API. Without it, there is no admin API.

```console
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" -d '{}' http://localhost:8080/v1/namespaces/my-db/create
```

Namespaces are always enabled, and there is no default namespace: the requests to the user API name theirs in the `x-namespace` header.

The description of the routes is generated from the code: it is served as an OpenAPI document at `/openapi.json`, and can be browsed at `/docs`.

## Routes

```HTTP
POST /v1/namespaces/:namespace/create
```

Create a namespace named `:namespace`.
body:

```json
{
    "dump_url"?: string,
}
```

```HTTP
DELETE /v1/namespaces/:namespace
```

Delete the namespace named `:namespace`.

```HTTP
POST /v1/namespaces/:namespace/fork/:to
```

Fork `:namespace` into new namespace `:to`

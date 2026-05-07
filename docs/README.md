# Documentation Index

## Start Here

Install and run locally:

```bash
make install
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

Deploy to Linux VPS with systemd user service:

```bash
make vps-deploy
```

Show all operational commands:

```bash
make help
```

## Guides

- [Install Guide](./INSTALL.md)
- [Deployment Guide](./DEPLOYMENT.md)
- [API Guide](./API.md)
- [Endpoint Catalog](./API_ENDPOINTS.md)
- [Architecture](./ARCHITECTURE.md)

## API Artifacts

- Generated OpenAPI artifact: [`openapi/runtime-server-openapi.yaml`](../openapi/runtime-server-openapi.yaml)
- Public OpenAPI endpoint: `GET /openapi.yaml`
- Authenticated OpenAPI endpoint: `GET /v1/openapi.yaml`

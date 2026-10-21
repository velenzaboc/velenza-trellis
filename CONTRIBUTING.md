# Contributing & Open-Source Hygiene Policy

Velenza Trellis (`velenza-trellis` / `task-graph` / `inr trellis`) is a strictly vendor-neutral, clean-room open-source project licensed under Apache-2.0.

## Strict Zero-Proprietary-IP & Config-Only Policy

To keep Velenza Trellis 100% portable and safe for public distribution, the repository enforces three non-negotiable rules through automated CI tests (`arbor_trellis_hygiene_test.go` `TestOpenSourceHygiene`):

1. **Zero Proprietary IP or Organization Identifiers**
   - No organization names, internal codenames, internal project identifiers, or proprietary domain names may appear anywhere in source code, comments, tests, commit messages, or documentation.
   - Use generic domains (`@example.com`) and generic project identifiers (`example-gcp-project`, `example-spanner-instance`, `example-org`) in unit and integration tests.

2. **Zero Hardcoded Environment Coordinates**
   - Never hardcode workstation paths (`/Users/...`, `/Volumes/...`, `/home/...`, `C:\Users\...`), internal hostnames, cloud project IDs, database instance IDs, or private/Tailscale IP addresses (`100.x.x.x`, `10.x.x.x`, `192.168.x.x`).

3. **Everything Goes Through Configuration (`TaskGraphConfig`)**
   - All deployment-specific coordinates—GCP project IDs, Spanner instance and database IDs, Vertex AI endpoints, and authentication credentials—must be supplied at runtime via `VELENZA_TRELLIS_CONFIG`, `TASK_GRAPH_CONFIG`, or `ARBOR_*` environment variables.
   - See [`velenza-trellis.config.example.json`](./velenza-trellis.config.example.json) for the full configuration schema.

## Verification Before Submitting Changes

Run the full test suite (including the open-source hygiene scanner) before opening a pull request:

```sh
go vet ./...
go build ./...
go test -race -count=1 ./...
```

## License, Code of Conduct & Contact

- **License**: By contributing to Velenza Trellis, you agree that your contributions will be licensed under the [Apache License, Version 2.0](./LICENSE) and subject to the attribution requirements in [`NOTICE`](./NOTICE).
- **Code of Conduct**: Participation is governed by our [Code of Conduct](./CODE_OF_CONDUCT.md).
- **Security Vulnerabilities**: Please follow [`SECURITY.md`](./SECURITY.md) and report security issues privately via GitHub Private Vulnerability Reporting or email to **open-source@velenzaboc.com**.
- **General Open-Source Contact**: **open-source@velenzaboc.com**

# Secure Web-Based Policy Ingestion

> Guidance for securely ingesting policy data from external web sources into the UM Claims Analytics pipeline.

---

## 1. Transport Security

- **HTTPS only** — all fetches use TLS 1.2+. Python's `httpx` or `requests` with `verify=True` (default) handles this.
- **Certificate pinning** (optional) — for known payer portals, pin the expected CA.

## 2. Authentication & Secrets

- **Never hardcode credentials.** Store API keys, OAuth client secrets, or bearer tokens in **Azure Key Vault**.
- Use `DefaultAzureCredential` from `azure-identity` to access Key Vault; this works on VDI (via managed identity or az CLI login) and in production (via managed identity).
- If payer portals use **OAuth 2.0**, do the client-credentials flow server-side and cache tokens with short TTLs.

## 3. Input Validation & Sanitization

- **Schema-gate all ingested content.** The pipeline already has `parse_policy_md.py` → structured JSON. Run that parser immediately after download — never pass raw web content directly to downstream stages.
- **Content-type checking** — reject unexpected MIME types before writing to disk.
- **Size limits** — cap download size to prevent resource exhaustion (e.g., 50 MB max).

## 4. Architecture Pattern

```
Payer Website / API
        │  HTTPS + OAuth / API key (from Key Vault)
        ▼
┌─────────────────────────┐
│  policy_web_loader.py   │  ← new module in src/um_claims/io/
│  - fetch with httpx     │
│  - verify TLS           │
│  - size / type checks   │
└────────┬────────────────┘
         │  raw Markdown / HTML
         ▼
┌─────────────────────────┐
│  Azure Storage Account  │  ← immutable blob (audit trail)
│  (existing in arch)     │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  parse_policy_md.py     │  ← existing parser / schema gate
│  → structured JSON      │
└─────────────────────────┘
```

## 5. Example Implementation Sketch

```python
"""Secure web-based policy document loader."""
from __future__ import annotations

import httpx
from pathlib import Path
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024  # 50 MB
ALLOWED_CONTENT_TYPES = {"text/markdown", "text/html", "text/plain", "application/pdf"}


def _get_secret(vault_url: str, secret_name: str) -> str:
    """Retrieve a secret from Azure Key Vault using managed identity."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value


def fetch_policy_document(
    url: str,
    *,
    vault_url: str | None = None,
    api_key_secret_name: str | None = None,
    timeout: float = 30.0,
) -> bytes:
    """Download a policy document from a URL with security controls.

    Args:
        url: HTTPS URL of the policy document.
        vault_url: Azure Key Vault URL (if auth required).
        api_key_secret_name: Name of the API key secret in Key Vault.
        timeout: Request timeout in seconds.

    Returns:
        Raw document bytes.

    Raises:
        ValueError: If URL is not HTTPS, content type is unexpected,
                    or response exceeds size limit.
        httpx.HTTPStatusError: On non-2xx responses.
    """
    if not url.startswith("https://"):
        raise ValueError(f"Only HTTPS URLs allowed, got: {url}")

    headers = {}
    if vault_url and api_key_secret_name:
        api_key = _get_secret(vault_url, api_key_secret_name)
        headers["Authorization"] = f"Bearer {api_key}"

    with httpx.Client(verify=True, timeout=timeout) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()

        # Content-type check
        ct = resp.headers.get("content-type", "").split(";")[0].strip()
        if ct not in ALLOWED_CONTENT_TYPES:
            raise ValueError(f"Unexpected content type: {ct}")

        # Size check
        if len(resp.content) > MAX_DOWNLOAD_BYTES:
            raise ValueError(f"Response too large: {len(resp.content)} bytes")

        return resp.content


def ingest_policy_from_web(
    url: str,
    output_dir: Path,
    **fetch_kwargs,
) -> Path:
    """Fetch a policy doc from the web, save to storage, return local path."""
    content = fetch_policy_document(url, **fetch_kwargs)

    # Save raw content for audit trail
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = url.rsplit("/", 1)[-1] or "policy_document.md"
    dest = output_dir / filename
    dest.write_bytes(content)

    return dest  # Caller then passes to parse_policy_md for schema validation
```

## 6. Additional Hardening for Production

| Concern | Mitigation |
|---|---|
| **Audit trail** | Write raw blobs to immutable Azure Storage before parsing |
| **Rate limiting** | Respect payer API rate limits; use exponential backoff |
| **Network isolation** | Run the fetcher inside a VNet with NSG rules limiting egress to known payer domains |
| **Logging** | Log URL, timestamp, content hash, and response status — never log credentials |
| **Retry with idempotency** | Use `tenacity` or `httpx` retries with jitter |
| **No PHI in transit** | Policy documents should not contain PHI; validate this as a post-download check |

---

This pattern slots directly into the existing pipeline — raw web content lands in Azure Storage (already in the architecture), then flows through `parse_policy_md.py` which acts as the schema gate before anything reaches the analytics stages.

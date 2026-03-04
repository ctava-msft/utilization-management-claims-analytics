# Secure Web-Based Policy Ingestion

> Guidance for securely ingesting policy data from external web sources into the UM Claims Analytics pipeline.

---

## Approach Comparison

There are several approaches to ingesting policy data from the web. The right choice depends on the source characteristics:

| Approach | Best For | Auth Portal Support | Output Fidelity | Infra Cost | Key Library |
|---|---|---|---|---|---|
| **A. Direct HTTP Fetch** | Known URLs / REST APIs | Yes (OAuth, API key) | Exact original document | ~$0 | `httpx` |
| **B. Browser Automation (Playwright)** | JS-rendered pages, login-gated portals | Yes (session cookies, MFA flows) | Exact rendered content | ~$0 | `playwright` |
| **C. AI Web Summarization (Bing + GPT)** | Discovery — unknown URLs, broad search | No (public web only) | LLM summary with citations | ~$57–220/mo | `azure-ai-projects` |
| **D. Azure AI Document Intelligence** | Scanned PDFs, images of policy docs | N/A (post-download) | Structured extraction from images/PDF | ~$1–50/mo | `azure-ai-documentintelligence` |
| **E. FoundryIQ Knowledge Index** | Semantic retrieval / RAG over ingested policies | N/A (post-ingestion) | Embedding-based semantic search + GPT synthesis | ~$50–150/mo | `azure-ai-projects` |

> **Recommendation:** Use **A** or **B** as the authoritative source of truth for known payer portals. Use **C** for discovery and enrichment from publicly available content. Use **D** as a post-download step when policies are distributed as scanned PDFs. Use **E** to build a persistent, searchable knowledge index over all ingested policies for downstream RAG queries. All approaches feed into the same `parse_policy_md.py` schema gate.

---

## Common Security Controls

### 1. Transport Security

- **HTTPS only** — all fetches use TLS 1.2+. Python's `httpx` or `requests` with `verify=True` (default) handles this.
- **Certificate pinning** (optional) — for known payer portals, pin the expected CA.

### 2. Authentication & Secrets

- **Never hardcode credentials.** Store API keys, OAuth client secrets, or bearer tokens in **Azure Key Vault**.
- Use `DefaultAzureCredential` from `azure-identity` to access Key Vault; this works on VDI (via managed identity or az CLI login) and in production (via managed identity).
- If payer portals use **OAuth 2.0**, do the client-credentials flow server-side and cache tokens with short TTLs.

### 3. Input Validation & Sanitization

- **Schema-gate all ingested content.** The pipeline already has `parse_policy_md.py` → structured JSON. Run that parser immediately after download — never pass raw web content directly to downstream stages.
- **Content-type checking** — reject unexpected MIME types before writing to disk.
- **Size limits** — cap download size to prevent resource exhaustion (e.g., 50 MB max).

### 4. Production Hardening

| Concern | Mitigation |
|---|---|
| **Audit trail** | Write raw blobs to immutable Azure Storage before parsing |
| **Rate limiting** | Respect payer API rate limits; use exponential backoff |
| **Network isolation** | Run the fetcher inside a VNet with NSG rules limiting egress to known payer domains |
| **Logging** | Log URL, timestamp, content hash, and response status — never log credentials |
| **Retry with idempotency** | Use `tenacity` or `httpx` retries with jitter |
| **No PHI in transit** | Policy documents should not contain PHI; validate this as a post-download check |

---

## Approach A — Direct HTTP Fetch

Best when you have **known URLs** or a **REST API** for payer policy documents.

### Architecture

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
└────────┬────────────────┘
         ▼
┌─────────────────────────┐
│  parse_policy_md.py     │  ← existing parser / schema gate
│  → structured JSON      │
└─────────────────────────┘
```

### Implementation Sketch

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

---

## Approach B — Browser Automation (Playwright)

Best for **JavaScript-rendered pages** or **login-gated payer portals** where a simple HTTP GET won't return the full content.

### When to Use Over Direct HTTP

- Payer policy pages are **single-page apps** (React, Angular) that render content client-side.
- Portal requires **session cookies** set by a login flow (form-based auth, SSO redirect, MFA).
- Policy documents are served behind **anti-bot protections** (CAPTCHAs, JS challenges).
- You need to **navigate multi-step workflows** (search → select policy → download PDF).

### Architecture

```
Payer Portal (JS-rendered)
        │  Headless Chromium via Playwright
        │  Session cookies / login flow
        ▼
┌──────────────────────────────┐
│  policy_browser_loader.py   │  ← new module in src/um_claims/io/
│  - playwright async API     │
│  - login automation          │
│  - wait for content render   │
│  - download / screenshot     │
└────────┬─────────────────────┘
         │  raw HTML / PDF
         ▼
┌──────────────────────────────┐
│  Azure Storage Account       │  ← immutable blob (audit trail)
└────────┬─────────────────────┘
         ▼
┌──────────────────────────────┐
│  parse_policy_md.py          │  ← existing parser / schema gate
│  → structured JSON           │
└──────────────────────────────┘
```

### Implementation Sketch

```python
"""Browser-based policy document loader using Playwright."""
from __future__ import annotations

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def _get_secret(vault_url: str, secret_name: str) -> str:
    """Retrieve a secret from Azure Key Vault."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value


async def fetch_policy_with_browser(
    url: str,
    *,
    vault_url: str | None = None,
    username_secret: str | None = None,
    password_secret: str | None = None,
    login_url: str | None = None,
    wait_selector: str = "body",
    timeout_ms: int = 30_000,
) -> str:
    """Fetch a policy page using a headless browser.

    Args:
        url: Target policy page URL.
        vault_url: Azure Key Vault URL (if login credentials needed).
        username_secret: Key Vault secret name for portal username.
        password_secret: Key Vault secret name for portal password.
        login_url: Portal login page URL (if auth required).
        wait_selector: CSS selector to wait for before capturing content.
        timeout_ms: Navigation timeout in milliseconds.

    Returns:
        Rendered HTML content of the page.

    Raises:
        ValueError: If URL is not HTTPS.
        TimeoutError: If page does not load within timeout.
    """
    if not url.startswith("https://"):
        raise ValueError(f"Only HTTPS URLs allowed, got: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            ignore_https_errors=False,  # enforce TLS validation
            user_agent="UMClaimsBot/1.0 (policy-ingestion)",
        )
        page = await context.new_page()

        # Optional: authenticate via login page
        if login_url and vault_url and username_secret and password_secret:
            username = _get_secret(vault_url, username_secret)
            password = _get_secret(vault_url, password_secret)
            await page.goto(login_url, timeout=timeout_ms)
            await page.fill('input[name="username"], input[type="email"]', username)
            await page.fill('input[name="password"], input[type="password"]', password)
            await page.click('button[type="submit"]')
            await page.wait_for_load_state("networkidle")

        # Navigate to policy page
        await page.goto(url, timeout=timeout_ms)
        await page.wait_for_selector(wait_selector, timeout=timeout_ms)

        content = await page.content()
        await browser.close()
        return content


def ingest_policy_from_browser(
    url: str,
    output_dir: Path,
    **fetch_kwargs,
) -> Path:
    """Fetch a policy page via browser, save to storage, return local path."""
    html = asyncio.run(fetch_policy_with_browser(url, **fetch_kwargs))

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = url.rsplit("/", 1)[-1] or "policy_page.html"
    if not filename.endswith(".html"):
        filename += ".html"
    dest = output_dir / filename
    dest.write_text(html, encoding="utf-8")

    return dest  # Caller passes to parse_policy_md for schema validation
```

### Dependencies

```bash
pip install playwright
playwright install chromium
```

### Security Considerations Specific to Playwright

| Concern | Mitigation |
|---|---|
| **Credential exposure** | Portal credentials stored in Key Vault, never in code or env vars |
| **Browser sandbox** | Run headless Chromium in a container with no-sandbox disabled; use `--disable-dev-shm-usage` for constrained environments |
| **Network egress** | Restrict browser traffic to allow-listed payer domains via proxy or VNet NSG |
| **Content injection** | Never execute untrusted JavaScript; capture `page.content()` only after load |
| **Resource limits** | Set navigation timeouts; kill browser process on timeout to prevent hangs |

---

## Approach C — AI Web Summarization (Bing Grounding + GPT)

Best for **policy discovery** — when you don't have exact URLs but need to find and summarize publicly available payer policies.

Reference implementation: [ctava-msft/web-summarization](https://github.com/ctava-msft/web-summarization)

### When to Use

- You need to **search** for policies across the open web (e.g., "UPMC prior auth policy for CPT 27447").
- You want **LLM-generated summaries** with citations for analyst review.
- You're enriching existing policy data with supplementary public information.

### Architecture

```
Natural-language query
        │
        ▼
┌──────────────────────────────────┐
│  Azure AI Foundry Agent          │
│  - GPT-5.2-mini model            │
│  - Bing Grounding tool           │
│  - Streaming responses           │
└────────┬─────────────────────────┘
         │  LLM summary + citation URLs
         ▼
┌──────────────────────────────────┐
│  Azure Storage Account           │  ← store summary for audit
└────────┬─────────────────────────┘
         ▼
┌──────────────────────────────────┐
│  Adapter → parse_policy_md.py    │  ← convert summary to canonical JSON
│  → structured JSON               │
└──────────────────────────────────┘
```

### Implementation Sketch

```python
"""Policy discovery via Azure AI Foundry with Bing Grounding."""
from __future__ import annotations

from pathlib import Path

from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    BingGroundingAgentTool,
    BingGroundingSearchConfiguration,
    BingGroundingSearchToolParameters,
    PromptAgentDefinition,
)
from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI


def search_and_summarize_policy(
    query: str,
    *,
    project_endpoint: str,
    model_deployment: str,
    bing_connection_id: str,
) -> dict[str, str | list[str]]:
    """Search the web for policy information and return an LLM summary.

    Args:
        query: Natural-language policy query.
        project_endpoint: Azure AI Foundry project endpoint.
        model_deployment: GPT model deployment name.
        bing_connection_id: Bing Grounding connection resource ID.

    Returns:
        Dict with keys 'summary' (str) and 'citations' (list[str]).
    """
    credential = DefaultAzureCredential()
    project_client = AIProjectClient(
        endpoint=project_endpoint, credential=credential
    )

    # Create agent with Bing Grounding
    agent = project_client.agents.create_version(
        agent_name="PolicyResearcher",
        definition=PromptAgentDefinition(
            model=model_deployment,
            instructions=(
                "You are a healthcare policy research assistant. "
                "Search for utilization management and prior authorization policies. "
                "Provide a structured summary with CPT codes, authorization requirements, "
                "and effective dates. Always cite your sources."
            ),
            tools=[
                BingGroundingAgentTool(
                    bing_grounding=BingGroundingSearchToolParameters(
                        search_configurations=[
                            BingGroundingSearchConfiguration(
                                project_connection_id=bing_connection_id
                            )
                        ]
                    )
                )
            ],
        ),
        description="Policy research assistant with Bing grounding",
    )

    try:
        # Query via OpenAI streaming
        openai_client = AzureOpenAI(
            azure_endpoint=project_endpoint,
            azure_ad_token_provider=credential.get_token,
        )
        stream = openai_client.responses.create(
            stream=True,
            input=query,
            extra_body={"agent": {"name": agent.name, "type": "agent_reference"}},
        )

        summary_parts: list[str] = []
        citations: list[str] = []
        for event in stream:
            if event.type == "response.output_text.delta":
                summary_parts.append(event.delta)
            elif event.type == "response.output_item.done":
                for annotation in getattr(event, "annotations", []):
                    if annotation.type == "url_citation":
                        citations.append(annotation.url)

        return {"summary": "".join(summary_parts), "citations": citations}
    finally:
        project_client.agents.delete_version(agent.name, agent.version)
```

### Infrastructure

Deployed via `azd up` using the [web-summarization](https://github.com/ctava-msft/web-summarization) template:

| Resource | Purpose |
|---|---|
| Azure AI Foundry (AIServices) | Unified AI account with built-in project |
| GPT-5.2-mini Deployment | Summarization model |
| Bing Grounding | Web search capabilities |
| Agents Capability | Runtime orchestration |

Estimated cost: **~$57–220/month** (Foundry + Bing API).

### Limitations

- **No authenticated portals** — Bing can only index publicly available content.
- **Fidelity risk** — LLM summaries may miss critical exclusion clauses or CPT modifier requirements. Always verify against authoritative sources.
- **Index freshness** — newly published or updated policies may not yet be in the Bing index.
- **Schema gap** — output is free-form text; requires an adapter layer to produce canonical policy JSON for `parse_policy_md.py`.

---

## Approach D — Azure AI Document Intelligence

Best as a **post-download processing step** when policies are distributed as scanned PDFs or images.

### When to Use

- Payer distributes policies as **scanned PDFs** (image-based, not text-selectable).
- Policy documents contain **tables, forms, or structured layouts** that need precise extraction.
- Used **after** Approach A or B has downloaded the raw file.

### Architecture

```
Downloaded PDF / image
  (from Approach A or B)
        │
        ▼
┌──────────────────────────────────┐
│  Azure AI Document Intelligence  │
│  - Layout / prebuilt-document    │
│  - OCR + table extraction        │
└────────┬─────────────────────────┘
         │  structured text / tables
         ▼
┌──────────────────────────────────┐
│  parse_policy_md.py              │  ← existing parser / schema gate
│  → structured JSON               │
└──────────────────────────────────┘
```

### Implementation Sketch

```python
"""Extract policy text from scanned PDFs using Azure AI Document Intelligence."""
from __future__ import annotations

from pathlib import Path

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.identity import DefaultAzureCredential


def extract_policy_from_pdf(
    pdf_path: Path,
    *,
    endpoint: str,
) -> str:
    """Extract text from a scanned policy PDF.

    Args:
        pdf_path: Path to the downloaded PDF.
        endpoint: Azure AI Document Intelligence endpoint.

    Returns:
        Extracted text content.
    """
    credential = DefaultAzureCredential()
    client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=credential
    )

    with open(pdf_path, "rb") as f:
        poller = client.begin_analyze_document(
            "prebuilt-layout",
            body=AnalyzeDocumentRequest(bytes_source=f.read()),
        )
    result = poller.result()

    # Concatenate extracted text from all pages
    lines: list[str] = []
    for page in result.pages:
        for line in page.lines:
            lines.append(line.content)

    return "\n".join(lines)
```

Estimated cost: **~$1–50/month** depending on volume (pricing per page).

---

## Approach E — FoundryIQ Knowledge Index

Best for building a **persistent, searchable knowledge base** over all ingested policy documents — enabling semantic retrieval (RAG) for downstream analytics, anomaly explanation, and copilot-assisted policy Q&A.

This approach is already shown in the [solution architecture](solution_architecture.md) as the optional Policy RAG path.

### When to Use

- You have a **growing corpus** of policy documents (from Approaches A–D) and need to query them semantically.
- Analysts or the pipeline need to answer questions like *"Which policies require prior auth for CPT 27447?"* without scanning every document.
- You want to **ground LLM responses** in authoritative policy text (RAG pattern) rather than relying on the model's training data.
- Copilot-assisted workflows on the VDI need **real-time policy context** during claims analysis.

### Architecture

```
Ingested policy documents
  (from Approaches A–D, stored in Azure Storage)
        │
        ▼
┌──────────────────────────────────────┐
│  Azure AI Foundry                    │
│  text-embedding-3 model              │
│  - Chunk documents                   │
│  - Generate vector embeddings        │
└────────┬─────────────────────────────┘
         │  embeddings + metadata
         ▼
┌──────────────────────────────────────┐
│  FoundryIQ Knowledge Index           │
│  - Vector store + metadata index     │
│  - Semantic search API               │
│  - Citation tracking                 │
└────────┬─────────────────────────────┘
         │  query-time retrieval
         ▼
┌──────────────────────────────────────┐
│  GPT-5.2-mini (RAG synthesis)        │
│  - Grounded in retrieved chunks      │
│  - Structured JSON output            │
│  - Source citations                  │
└────────┬─────────────────────────────┘
         ▼
┌──────────────────────────────────────┐
│  parse_policy_md.py                  │  ← schema gate for structured output
│  → canonical JSON                    │
└──────────────────────────────────────┘
```

### Implementation Sketch

```python
"""FoundryIQ-based policy knowledge index and semantic retrieval."""
from __future__ import annotations

from pathlib import Path

from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential


def index_policy_documents(
    policy_dir: Path,
    *,
    project_endpoint: str,
    index_name: str = "policy-knowledge",
) -> str:
    """Index policy documents into a FoundryIQ knowledge index.

    Reads all Markdown/text policy files from the directory, chunks them,
    generates embeddings via text-embedding-3, and upserts into the index.

    Args:
        policy_dir: Directory containing parsed policy documents.
        project_endpoint: Azure AI Foundry project endpoint.
        index_name: Name for the FoundryIQ knowledge index.

    Returns:
        The index name for downstream queries.
    """
    credential = DefaultAzureCredential()
    client = AIProjectClient(endpoint=project_endpoint, credential=credential)

    # Collect policy files
    files = list(policy_dir.glob("**/*.md")) + list(policy_dir.glob("**/*.txt"))
    if not files:
        raise FileNotFoundError(f"No policy documents found in {policy_dir}")

    # Create or update the knowledge index
    # FoundryIQ handles chunking, embedding, and indexing internally
    index = client.indexes.create_or_update(
        name=index_name,
        description="UM policy documents for semantic retrieval",
        embedding_model="text-embedding-3",
    )

    for file_path in files:
        content = file_path.read_text(encoding="utf-8")
        client.indexes.upload_document(
            index_name=index_name,
            document_id=file_path.stem,
            content=content,
            metadata={
                "source_file": file_path.name,
                "source_path": str(file_path),
            },
        )

    return index_name


def query_policy_index(
    query: str,
    *,
    project_endpoint: str,
    model_deployment: str = "GPT-5.2-mini",
    index_name: str = "policy-knowledge",
    top_k: int = 5,
) -> dict[str, str | list[str]]:
    """Query the FoundryIQ policy index with RAG synthesis.

    Args:
        query: Natural-language policy question.
        project_endpoint: Azure AI Foundry project endpoint.
        model_deployment: GPT model for synthesis.
        index_name: FoundryIQ knowledge index name.
        top_k: Number of chunks to retrieve.

    Returns:
        Dict with 'answer' (str), 'citations' (list[str]),
        and 'chunks' (list[str]) for traceability.
    """
    credential = DefaultAzureCredential()
    client = AIProjectClient(endpoint=project_endpoint, credential=credential)

    # Retrieve relevant chunks from the knowledge index
    search_results = client.indexes.query(
        index_name=index_name,
        query=query,
        top_k=top_k,
    )

    # Build grounded prompt with retrieved context
    context_chunks = []
    citations = []
    for result in search_results.results:
        context_chunks.append(result.content)
        citations.append(result.metadata.get("source_file", "unknown"))

    grounded_prompt = (
        "You are a healthcare policy analyst. Answer the question based ONLY on "
        "the provided policy excerpts. Cite which policy document each claim comes from.\n\n"
        "Policy excerpts:\n"
        + "\n---\n".join(
            f"[{citations[i]}]\n{chunk}"
            for i, chunk in enumerate(context_chunks)
        )
        + f"\n\nQuestion: {query}"
    )

    # Synthesize answer via GPT
    from openai import AzureOpenAI

    openai_client = AzureOpenAI(
        azure_endpoint=project_endpoint,
        azure_ad_token_provider=credential.get_token,
    )
    response = openai_client.chat.completions.create(
        model=model_deployment,
        messages=[{"role": "user", "content": grounded_prompt}],
        temperature=0,
    )

    return {
        "answer": response.choices[0].message.content,
        "citations": list(set(citations)),
        "chunks": context_chunks,
    }
```

### Infrastructure

| Resource | Purpose |
|---|---|
| Azure AI Foundry (AIServices) | Hosts the project, embedding model, and GPT |
| text-embedding-3 Deployment | Generates vector embeddings for policy chunks |
| FoundryIQ Knowledge Index | Persistent vector store with semantic search API |
| GPT-5.2-mini Deployment | Synthesizes grounded answers from retrieved chunks |

Estimated cost: **~$50–150/month** (embedding generation + index storage + GPT queries).

### How It Complements Other Approaches

| Ingestion Approach | FoundryIQ Role |
|---|---|
| **A. Direct HTTP / B. Playwright** | Index the downloaded authoritative documents for semantic search |
| **C. Bing + GPT** | Compare web-discovered policies against the indexed knowledge base |
| **D. Document Intelligence** | Index the OCR-extracted text from scanned PDFs |

FoundryIQ acts as the **long-term memory layer** — policies are ingested once (via A–D) and queried many times by analysts, the CLI pipeline, or copilot-assisted workflows on the VDI.

### Security Considerations

| Concern | Mitigation |
|---|---|
| **Data at rest** | FoundryIQ encrypts index data with Azure-managed or customer-managed keys |
| **Access control** | RBAC on the AI Foundry project; only authorized identities can query the index |
| **No PHI** | Same boundary as the rest of the pipeline — only de-identified policy text is indexed |
| **Audit** | All queries and index mutations logged via Azure Monitor / Diagnostic Settings |
| **Grounding fidelity** | RAG responses include source citations; analysts can verify against original documents in Azure Storage |

---

## Combined Architecture — All Approaches

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           Policy Ingestion Layer                                 │
├──────────────────┬──────────────────┬────────────────┬───────────────────────────┤
│  A. Direct HTTP  │  B. Playwright   │  C. Bing+GPT   │  D. Document Intelligence │
│  (known URLs)    │  (JS / portals)  │  (discovery)   │  (scanned PDFs)           │
└────────┬─────────┴────────┬─────────┴───────┬────────┴──────────────┬────────────┘
         │                  │                 │                       │
         ▼                  ▼                 ▼                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Azure Storage Account                                    │
│                         (immutable blobs — audit trail)                           │
└──────────────────────────────┬───────────────────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
┌──────────────────────────────┐  ┌───────────────────────────────────────────────┐
│  parse_policy_md.py          │  │  E. FoundryIQ Knowledge Index                 │
│  → canonical structured JSON │  │  - text-embedding-3 → vector index            │
└──────────────────────────────┘  │  - Semantic search (RAG)                      │
                                  │  - GPT-5.2-mini grounded synthesis            │
                                  │  → query-time policy retrieval & Q&A          │
                                  └───────────────────────────────────────────────┘
```

Approaches A–D converge at Azure Storage. From there, content flows to:
- **`parse_policy_md.py`** — the schema gate producing canonical JSON for the analytics pipeline.
- **FoundryIQ (E)** — the long-term semantic memory layer enabling RAG-based policy queries, copilot grounding, and analyst Q&A.

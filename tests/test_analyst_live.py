"""The Analyst against the real Claude API. Skipped unless AGB_LIVE_ANTHROPIC=1, with
ANTHROPIC_API_KEY in the environment (never in a file in the repository):

    AGB_LIVE_ANTHROPIC=1 uv run pytest -m live -s tests/test_analyst_live.py

One Haiku 4.5 call over the fake EDGAR's filings, a few thousand tokens. It checks what the
fake model cannot: that the API accepts the request as built (temperature 0 in the body, the
JSON schema, the cached system prompt), that the answer parses, and that the model's quotes
verify against the documents they cite."""

import os

import pytest
from edgar_fake import AS_OF, client

from agb import analyst, edgar

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_ANTHROPIC") != "1", reason="set AGB_LIVE_ANTHROPIC=1 to run"
    ),
]


def test_the_analyst_quotes_verbatim():
    retrieval = edgar.retrieve(client(), "ACME", AS_OF)
    docs, _ = analyst.documents(
        [(s.filing.form, s.filing.filed.isoformat(), s.section, s.text) for s in retrieval.sections]
    )

    completion = analyst.AnthropicModel().complete(analyst.request(retrieval.name, "ACME", docs))
    quotes = analyst.verify(analyst.parse(completion), docs)

    print(completion)
    for quote in quotes:
        print(quote)
    assert completion.model.startswith("claude-haiku-4-5")
    assert completion.stop_reason == "end_turn"
    assert any(q.verified for q in quotes)
    assert {q.dimension for q in quotes if q.verified} >= {1, 2, 4}

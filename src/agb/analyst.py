"""The panel's Analyst: verbatim quotes, with citations, against the six rubric dimensions.

The Analyst reads the named company's filings and transcribes the passages that bear on each
rubric dimension. It does not summarise and does not judge (docs/architecture/03). Each quote
cites the document it came from, and the section within it, and is checked here against that
document's text. A quote that is not in its source verbatim is rejected: it is a
hallucination, and everything downstream would be scoring fiction.

The request is fixed by the documents: the model, temperature 0, a system prompt that is the
same for every name (so prompt caching serves it), and the documents in reading order. Its
cache key is the SHA-256 of the request, so an identical request is answered from the panel
store instead of the API (agb.panel).
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Protocol

MODEL = "claude-haiku-4-5"
# The debate roles belong to T24; their defaults live here so panel_config has one source.
DEBATE_MODELS = {"prosecutor": "claude-sonnet-5", "defender": "claude-sonnet-5"}
JUDGE_MODEL = "claude-opus-5"
MAX_TOKENS = 16000
# The most document text one request carries, in characters: about 150k tokens, inside Haiku
# 4.5's 200k context with room for the prompt and the answer.
MAX_CHARS = 600_000

RUBRIC_VERSION = "1"
RUBRIC = {
    1: (
        "Going concern and liquidity",
        "substantial-doubt language, covenant issues or breaches, funding runway",
    ),
    2: (
        "Accounting integrity",
        "restatement, material weakness, auditor change, revenue-recognition change",
    ),
    3: ("Litigation and regulatory", "material, non-boilerplate legal or regulatory exposure"),
    4: (
        "Business-model impairment",
        "customer or supplier concentration, stated secular decline",
    ),
    5: (
        "Governance and management",
        "CFO or executive turnover, related-party transactions",
    ),
    6: (
        "Disclosure delta",
        "new or materially expanded risk factors, a shift in the tone of MD&A",
    ),
}

SYSTEM = """\
You are the Analyst on a panel that screens companies for qualitative impairment: whether \
something in the company's recent SEC filings makes it dangerous in a way financial ratios \
cannot show.

Your only job is retrieval. Read the filings supplied and copy out the passages that bear on \
each rubric dimension below. Do not summarise, paraphrase, interpret or score anything; later \
roles do that from your quotes alone.

Rubric dimensions:
{rubric}

Rules for every quote:
- Copy it character for character from one document. It is checked by exact string match \
against that document, and any quote that does not match is discarded.
- Keep each quote to one to three contiguous sentences. Never join text from different places \
with an ellipsis.
- Cite the document by its id attribute, and name the section or heading it appears under.
- Prefer specific disclosures (amounts, dates, named events) over generic risk language that \
any company could have written.
- A dimension with nothing relevant gets no quotes. Do not invent evidence to fill it.

Answer with JSON only, in this shape:
{{"quotes": [{{"dimension": 1, "document": "D1", "section": "Liquidity and Capital \
Resources", "quote": "..."}}]}}
"""

USER = """\
Company: {company} (ticker {ticker})

{documents}

Extract the verbatim quotes for each rubric dimension."""

DOCUMENT = (
    '<document id="{id}" form="{form}" filed="{filed}" section="{section}">\n{text}\n</document>'
)

SCHEMA = {
    "type": "object",
    "properties": {
        "quotes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dimension": {"type": "integer"},
                    "document": {"type": "string"},
                    "section": {"type": "string"},
                    "quote": {"type": "string"},
                },
                "required": ["dimension", "document", "section", "quote"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["quotes"],
    "additionalProperties": False,
}


def system_prompt() -> str:
    rubric = "\n".join(f"{n}. {title}: {what}" for n, (title, what) in RUBRIC.items())
    return SYSTEM.format(rubric=rubric)


def template_hash() -> str:
    """The SHA-256 of the Analyst's prompt templates, as recorded in panel_config."""
    templates = json.dumps([SYSTEM, USER, DOCUMENT, SCHEMA], sort_keys=True)
    return hashlib.sha256(templates.encode()).hexdigest()


class AnalystError(Exception):
    """A call that failed, or an answer that is not the JSON asked for."""


@dataclass(frozen=True)
class Document:
    id: str
    form: str
    filed: str
    section: str
    text: str


@dataclass(frozen=True)
class Completion:
    """One model answer, and what it cost."""

    text: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    stop_reason: str | None = None
    request_id: str | None = None


class Model(Protocol):
    """What the Analyst needs from Claude; tests pass a fake."""

    def complete(self, request: dict[str, Any]) -> Completion: ...


class AnthropicModel:
    """The Messages API through the official SDK. The key comes from ANTHROPIC_API_KEY."""

    def __init__(self, client: Any = None) -> None:
        if client is None:
            import anthropic

            client = anthropic.Anthropic()
        self._client = client

    def complete(self, request: dict[str, Any]) -> Completion:
        import anthropic

        params = dict(request)
        # SDK 1.x takes no sampling arguments; Haiku 4.5 still honours temperature in the body.
        extra = {"temperature": params.pop("temperature")} if "temperature" in params else {}
        try:
            response = self._client.messages.create(**params, extra_body=extra or None)
        except anthropic.APIError as exc:
            raise AnalystError(f"{request['model']}: {type(exc).__name__}: {exc}") from None
        usage = response.usage
        return Completion(
            text="".join(block.text for block in response.content if block.type == "text"),
            model=response.model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_input_tokens or 0,
            cache_creation_tokens=usage.cache_creation_input_tokens or 0,
            stop_reason=response.stop_reason,
            request_id=getattr(response, "_request_id", None),
        )


# Forms cut first when the documents do not fit, the oldest of them first.
_CUT_FIRST = ("8-K",)


def documents(sections: list[tuple[str, str, str, str]]) -> tuple[list[Document], list[int]]:
    """Number the (form, filed, section, text) pieces D1, D2, ... in reading order, cutting
    the text to MAX_CHARS in total. Returns the documents and the characters each one sent.

    The periodic filings take the budget first, in reading order. The 8-Ks share what is left,
    newest first, so that it is the oldest 8-Ks that are cut."""
    periodic = [n for n, piece in enumerate(sections) if piece[0] not in _CUT_FIRST]
    current = [n for n, piece in enumerate(sections) if piece[0] in _CUT_FIRST]
    current.sort(key=lambda n: (sections[n][1], n), reverse=True)
    left, take = MAX_CHARS, {}
    for n in periodic + current:
        take[n] = min(len(sections[n][3]), max(0, left))
        left -= take[n]
    out, sent = [], []
    for n, (form, filed, section, text) in enumerate(sections):
        sent.append(take[n])
        if take[n]:
            out.append(Document(f"D{n + 1}", form, filed, section, text[: take[n]]))
    return out, sent


def request(company: str, ticker: str, docs: list[Document], model: str = MODEL) -> dict[str, Any]:
    """The Messages API request for one name: deterministic for the same documents."""
    body = "\n\n".join(
        DOCUMENT.format(id=d.id, form=d.form, filed=d.filed, section=_attr(d.section), text=d.text)
        for d in docs
    )
    return {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "temperature": 0,
        "system": [
            {"type": "text", "text": system_prompt(), "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {
                "role": "user",
                "content": USER.format(company=company, ticker=ticker, documents=body),
            }
        ],
        "output_config": {"format": {"type": "json_schema", "schema": SCHEMA}},
    }


def _attr(text: str) -> str:
    return text.replace('"', "'")


def cache_key(req: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(req)).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@dataclass(frozen=True)
class Quote:
    dimension: int | None
    document: str
    section: str
    text: str
    verified: bool
    rejection: str | None = None


def parse(completion: Completion) -> list[dict[str, Any]]:
    """The quotes in an answer. An answer cut off or not in the asked-for shape is an error."""
    if completion.stop_reason == "max_tokens":
        raise AnalystError("the answer was cut off at max_tokens")
    text = completion.text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    try:
        answer = json.loads(text)
    except ValueError:
        raise AnalystError("the answer is not JSON") from None
    quotes = answer.get("quotes") if isinstance(answer, dict) else None
    if not isinstance(quotes, list) or not all(isinstance(q, dict) for q in quotes):
        raise AnalystError("the answer has no list of quotes")
    return quotes


def verify(raw: list[dict[str, Any]], docs: list[Document]) -> list[Quote]:
    """Each quote checked against the document it cites. Only verified quotes go downstream."""
    by_id = {d.id: normalise(d.text) for d in docs}
    out = []
    for item in raw:
        dimension = item.get("dimension")
        document = str(item.get("document") or "")
        section = str(item.get("section") or "").strip()
        text = str(item.get("quote") or "").strip()
        rejection = None
        if not isinstance(dimension, int) or isinstance(dimension, bool) or dimension not in RUBRIC:
            rejection = f"no rubric dimension {dimension!r}"
            dimension = None
        elif document not in by_id:
            rejection = f"cites no supplied document ({document or 'none'})"
        elif not section:
            rejection = "cites no section"
        elif not text:
            rejection = "empty quote"
        elif normalise(text) not in by_id[document]:
            rejection = f"not found verbatim in {document}"
        out.append(Quote(dimension, document, section, text, rejection is None, rejection))
    return out


_TYPOGRAPHY = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-", "—": "-"})


def normalise(text: str) -> str:
    """Text as compared for verbatim matching: Unicode NFKC, straight quotes and dashes, and
    every run of whitespace a single space. Nothing else is forgiven: case, words, punctuation
    and order must all match."""
    text = unicodedata.normalize("NFKC", text).translate(_TYPOGRAPHY)
    return " ".join(text.split())

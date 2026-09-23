"""The debate against the real Claude API. Skipped unless AGB_LIVE_ANTHROPIC=1, with
ANTHROPIC_API_KEY in the environment (never in a file in the repository):

    AGB_LIVE_ANTHROPIC=1 uv run pytest -m live -s tests/test_debate_live.py

One Prosecutor and one Defender call on Sonnet 5 and one Judge call on Opus 5, over a small
scrubbed extract written here, a few thousand tokens each. It checks what the fake
models cannot: that the API accepts the requests as built (no sampling parameters, the cached
system prompts, the Judge's JSON schema), that the advocates answer in full, and that the Judge's
answer parses into six scored and reasoned dimensions that code sums."""

import os

import pytest

from agb import analyst, debate
from agb.scrubber import SCRUBBER_VERSION

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_ANTHROPIC") != "1", reason="set AGB_LIVE_ANTHROPIC=1 to run"
    ),
]

SECTOR = "Industrials"
EXTRACT = {
    "rubric_version": analyst.RUBRIC_VERSION,
    "scrubber_version": SCRUBBER_VERSION,
    "quotes": [
        {
            "dimension": 1,
            "form": "10-K",
            "filed": "2026-02-20",
            "section": "Item 7",
            "quote": "As of December 31, 2025, the Company was not in compliance with the "
            "leverage covenant under its revolving credit facility",
        },
        {
            "dimension": 2,
            "form": "10-Q",
            "filed": "2026-05-01",
            "section": "Item 4",
            "quote": "we identified a material weakness in our internal control over financial "
            "reporting",
        },
        {
            "dimension": 4,
            "form": "10-K",
            "filed": "2026-02-20",
            "section": "Item 1A",
            "quote": "accounted for 62% of our revenue in fiscal 2025, and the loss of that "
            "customer would materially harm the Company.",
        },
    ],
}


def test_the_panel_debates_and_the_judge_scores_six_dimensions():
    model = analyst.AnthropicModel()
    status = debate.status(None, False)
    models = {**analyst.DEBATE_MODELS, "judge": analyst.JUDGE_MODEL}

    prosecuted = model.complete(
        debate.prosecutor_request(models["prosecutor"], SECTOR, status, EXTRACT)
    )
    prosecution = debate.answer(prosecuted)
    defended = model.complete(
        debate.defender_request(models["defender"], SECTOR, status, EXTRACT, prosecution)
    )
    defence = debate.answer(defended)
    completion = model.complete(
        debate.judge_request(models["judge"], SECTOR, status, EXTRACT, prosecution, defence)
    )
    result = debate.score(debate.parse(completion), None, False)

    print(prosecution, defence, completion, *result.dimensions, sep="\n\n")
    assert prosecuted.model.startswith("claude-sonnet-5")
    assert defended.model.startswith("claude-sonnet-5")
    assert completion.model.startswith("claude-opus-5")
    assert completion.stop_reason == "end_turn"
    assert [d.dimension for d in result.dimensions] == [1, 2, 3, 4, 5, 6]
    assert all(d.reason for d in result.dimensions)
    assert 0 <= result.value <= 1

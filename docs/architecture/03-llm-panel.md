# 03 — LLM Panel

**Source:** [#6 Design LLM panel signal output (TradingAgents-pattern feature generator)](https://github.com/agb1986/agb-trader/issues/6), amended by [#8](https://github.com/agb1986/agb-trader/issues/8) (Judge reason strings) and [#12](https://github.com/agb1986/agb-trader/issues/12) (panel configuration, shadow baskets, panel logs)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

The panel is a **qualitative impairment detector emitting a safety score**, not a return forecaster. It satisfies the consumer-side contract declared in [02 — Classical ranker](02-classical-ranker.md): it scores only the ~50 shortlisted names per monthly rebalance, emits one scalar per name, has that scalar percentile-ranked across the shortlist by stage 2, defaults to neutral on failure, and never sees the stage-1 rank.

## What question the panel answers

The panel assesses whether a name is **qualitatively impaired**: whether something in the recent public record makes it dangerous in a way the stage-1 factors cannot detect. Fraud allegations, going-concern language, auditor changes, restatements, material weakness disclosures, litigation reserves, revenue-recognition changes, management exodus, customer concentration.

Rejected: forward-return conviction. That puts the panel in direct competition with the ranker on the ranker's own turf, producing two estimators of the same quantity; the blend would average them, adding variance and API spend without new information. It is also the question LLMs are weakest at and the one most exposed to hindsight contamination. Rejected: news-flow sentiment, a commodity signal that decays within hours to days and cannot be harvested by a monthly rebalance.

The impairment question targets the specific hole a factor model has. The stage-1 screen buys cheap, profitable, high-momentum small caps, and cheap small caps are where accounting frauds and structurally impaired businesses cluster. That is the value trap, and it is invisible to a ratio, because the ratio looks excellent right up until the restatement. It is also the safer question epistemically: "what risks are stated in this document" is grounded in text supplied to the model, whereas "will this outperform" invites it to reach for what it remembers.

The resulting scalar is a **safety score**, so `stage1_rank_pct + w * llm_score_pct` reads as: promote names the factors like *and* the filings do not flag.

## Implementation base: bespoke

TradingAgents is design input, not code. Its graph is a hardcoded LangGraph `StateGraph` with no registration mechanism for custom roles; its output is a five-tier categorical trade decision, which is both the tie-heavy degenerate output stage 2 cannot use and exactly the call the panel is forbidden from making; and its data vendors have no point-in-time gating. A fork would delete most of the graph while inheriting LangGraph, its state schema, memory system and CLI, to keep a role pattern that amounts to a few hundred lines of prompt orchestration.

Taken from TradingAgents as design input: the deep/quick model split by role, bounded debate rounds, and structured per-role reports feeding an adjudicator.

## Role structure

**Analyst → Prosecutor / Defender → Judge.** One debate round.

- **Analyst** — retrieves the filings and emits verbatim quotes with section citations against each rubric dimension. It does not summarise and does not judge.
- **Prosecutor** — argues the name is impaired, from the extract.
- **Defender** — argues the concerns are boilerplate or already priced.
- **Judge** — weighs the exchange and scores the rubric, with one short reason string per dimension.

The `trader` and risk-management roles from the TradingAgents pattern are deliberately absent: they make a buy/sell call, which is the architectural boundary this design is built around. Bull/bear is replaced by prosecutor/defender because the bull/bear pair argues about *direction* while this panel argues about *impairment*.

The adversarial structure exists for one concrete reason: **Item 1A risk factors are boilerplate.** Every 10-K states that competition is intense, key personnel may depart, cyber incidents may occur and the business may fail. A single agent reading that either flags every company or learns to ignore the section and flags none; either way the score stops discriminating, which is the only property stage 2 needs from it. The Defender's specific job is to say "this language appears in every filing in this sector; it is not information." Whether something is boilerplate is a comparative judgement, not a checklist item.

A single Analyst covers all sources rather than one per source type. Splitting multiplies cost for marginal gain when the downstream consumer is a single scalar.

## Sources

**SEC filings only**, via EDGAR:

- Latest 10-K, Item 1A (risk factors) and Item 7 (MD&A)
- Latest 10-Q
- Any 8-K filed since

EDGAR's `recent` list holds only a filer's newest filings, so for a filer busy enough that it does not reach a 10-K, the older submission pages are read until one does. When the documents together exceed the Analyst's request budget, the periodic filings keep their place and the 8-Ks share the rest newest first: the oldest 8-Ks are cut, never the latest.

Filings are the highest-signal source for the impairment question, they are free, and each carries an unambiguous filing date, so point-in-time gating is exact and matches the `datekey` discipline applied to Sharadar SF1.

News is deferred to a later, separately logged experiment. It is where contamination and cost bite hardest: publication dates are frequently unreliable, aggregators backfill and revise, and a news API adds a paid dependency for a signal that largely restates what the filing already said.

## Output: rubric, scored 0–4 per dimension, summed in code

The Judge scores six dimensions, 0 to 4 each, where 0 is severe concern and 4 is clean:

1. **Going concern and liquidity** — substantial-doubt language, covenant issues, funding runway
2. **Accounting integrity** — restatement, material weakness, auditor change, revenue-recognition change
3. **Litigation and regulatory** — material, non-boilerplate exposure
4. **Business-model impairment** — customer or supplier concentration, stated secular decline
5. **Governance and management** — CFO or executive turnover, related-party transactions
6. **Disclosure delta** — new or materially expanded risk factors versus the prior year's filing, tone shift in MD&A

The sum runs 0–24 and is normalised to [0, 1], higher meaning safer. **Aggregation happens in code, not in the model**: no arithmetic errors, fully deterministic, reproducible from the logged sub-scores.

**Alongside each score the Judge emits one short reason string for that dimension.** Six bare integers explain nothing to someone reading a phone at 15:40: "accounting integrity 1/4" prompts the question it should be answering. The Judge is already reasoning its way to each score and is only being asked to keep one sentence of it, but this has to be built in rather than retrofitted, because a score without its reason cannot be explained after the fact. The Telegram `/why` drill-down prints the reason for any dimension scoring 2 or below; the local UI's panel view shows all six.

Rejected: a single 0–100 score. LLMs are poorly calibrated on fine numeric scales; output clusters on round numbers, so the resolution is illusory and unstable across runs. Rejected: a single 1–5 rating, which reproduces the tie-heavy degenerate case at fifty names.

The rubric also supplies **explainability** for the approval flow and the UI. A bare 63 tells the approving human nothing; "accounting integrity 1/4, everything else clean" tells them exactly where to look. Dimension 6 is the structural defence against boilerplate: comparing this year's filing against the same company's prior filing converts an absolute reading problem into a delta, and a newly added or materially expanded risk factor is real information in a way the standing litany never is.

## Model tiering and cost control

Token shape drives this. The Analyst reads full filings (roughly 5,000–20,000 tokens per name); Prosecutor, Defender and Judge read the Analyst's extract (roughly 2,000–3,000 tokens). At 50 names monthly that is approximately 1M input tokens for the Analyst against roughly 450k for the other three combined. The Analyst is the cost centre.

- **Analyst** — Haiku 4.5
- **Prosecutor, Defender** — Sonnet 5
- **Judge** — Opus 5

A cheap Analyst would be a single point of blindness if it wrote summaries. It does not: it emits verbatim quotes with citations, which is retrieval and faithful transcription, work a small fast model does well and where hallucination is mechanically checkable because every extract must quote text that exists in the filing. The Judge takes the strongest model because it is calibration-critical; the scalar's entire value is consistent discrimination across fifty names.

**Debate rounds: 1.** The Analyst's extract is fixed, so a second round argues over the same quotes at double the cost.

**Determinism is mandatory.** Temperature 0, minimum reasoning variance, and every prompt and response logged with its model ID. The `w=0` versus `w>0` comparison only measures the panel if the panel is reproducible, and a live/paper discrepancy that cannot be replayed cannot be diagnosed.

**Prompt caching, not the Batch API.** The Batch API's discount carries a 24-hour turnaround, and the window is tighter than that: the shortlist exists only after the month-end close, and MOC orders must be submitted before 15:45 ET the following session. Prompt caching applies immediately to every call, and the shared rubric and instruction block is identical across all fifty names.

**Configuration is immutable and versioned.** A `panel_config` row holds the model ID per role, prompt-template hashes, rubric version, scrubber version, shortlist size N and `w`. Changing any of them creates a new row through the server CLI, and every basket references the panel configuration it was built with.

## Blinding: identified retrieval, blind judgement

The Analyst sees the company's identity, because it must in order to fetch and cite the filing. Its extract then passes through a **deterministic scrubber** before Prosecutor, Defender and Judge see it: company name, ticker and the aliases listed on the filing's cover page are replaced with "the Company", which is how filings already refer to themselves most of the time. Plain string substitution, no model involved, fully replayable.

This matters even though the panel never runs on historical dates. Told it is looking at a specific named company, a model reasons partly from what it remembers about that company rather than from the document supplied, and there is then no way to tell whether the score reflects the filing or the reputation.

Rejected: full disclosure with a prompt instruction to judge only from provided text; "ignore what you know" is unenforceable and unmeasurable. Rejected: anonymising the filing itself, which names the company hundreds of times along with products, subsidiaries and executives. Scrubbing works here only because it operates on the small extract.

**Sector is passed explicitly** to the debate roles, recovering the interpretive frame (a biotech's risk factors read differently from a regional bank's) without restoring identity.

Every message that shows a panel score to a human states that the panel scored a name-scrubbed extract and did not know which company it was looking at. This also enables a later experiment: scrubbed versus unscrubbed on the same shortlist measures how much of the panel's output was memory rather than reading.

## Missing and stale filings

Governing principle: **absence of data is neutral; evidence of delinquency is a finding.**

- **EDGAR unreachable, parse failure, malformed document** — the system broke, not the company. Neutral 0.5 per the ranker's failure rule. Logged. A transient error (HTTP 429 or 5xx, a dropped connection) is first retried a few times with a growing pause, each attempt inside the fair-access rate limit, so one 503 does not cost a name its score.
- **A 10-K or prospectus whose section heading is not found** — the filer's wording, not a broken system. The whole document is read instead and the section is labelled "Whole document (… not found)", so the name is still scored.
- **Delinquent filer** — a 10-K more than roughly 15 months past fiscal year end, or an NT 10-K (late-filing notification) on file. This is not missing data, it is data. An NT 10-K is among the strongest distress signals in the public record and routinely precedes restatements, auditor resignations and delistings. It scores directly into dimensions 1 and 2.
- **Recent IPO with no 10-K yet** — score from the S-1 or prospectus risk factors. Dimension 6 has no prior filing to compare against and scores a neutral 2.

Rejected: dropping unscored names from the shortlist. That changes shortlist size and silently biases the portfolio toward companies with clean filing histories, a selection effect that would appear in no log. The panel must not edit the shortlist behind stage 2's back.

## Pre-flight validation

The panel is committed to forward-only return validation, which is honest but slow. These checks run before and alongside it, separating *is it reading correctly* (testable now, mechanically) from *does reading correctly help returns* (testable only forward).

- **Retrieval fidelity** — automated and contamination-free, because it tests transcription rather than prediction. Every quote the Analyst emits must be findable verbatim in the source document. A quote that is not in the filing is a hallucination, and everything downstream is then scoring fiction.
- **Known-disclosure recall** — take companies with known filed disclosures (a going-concern qualification, an NT 10-K, a disclosed material weakness) and check that the Analyst surfaces them. This asks "did it find what is in the document", not "did it know what would happen".
- **Human spot-check** on roughly 20 names, catching gross miscalibration that automated tests miss.
- **Dispersion gate**, built first. It yields a usefulness signal in month one, before any returns exist: if the panel scores every name 18–20 out of 24 it is useless regardless of individual correctness, because stage 2 percentile-ranks the output and a flat distribution ranks to noise. Measure score dispersion on the first live shortlist; low dispersion means stop and fix the rubric. The pre-trade gate in [04 — Risk limits and approval workflow](04-risk-and-approval.md) checks dispersion every month thereafter.

Explicitly **not evidence**: scoring the panel against subsequent returns on historical names. That is the contaminated historical-outcome test, and it would measure the model's memory.

**Results belong to the configuration that produced them.** Each check records what it measured against what it required, append-only and with an actor, under one `panel_config`; the newest result per check is the one that counts, so a re-run after a rubric fix replaces nothing and still lifts the gate. A configuration nobody has pre-flighted reads as not yet pre-flighted rather than as passed, which is why changing a model or a prompt means flying the suite again. The operator drives all four from the CLI (`agb panel-preflight`), and the spot-check presents one name at a time with its scores, its reasons and the quotes the debate saw, recording a verdict per name with the operator's own name on it.

Passing these pre-flight checks, including the dispersion gate, is one of the paper-to-live criteria.

## Running the w=0 vs w>0 comparison

**One live portfolio plus an exact recorded counterfactual.** The live portfolio trades `w = 0`, the validated ranker. The `w > 0` blend is computed from the same shortlist and marked to the same official closing prices, and recorded alongside as a **shadow basket series** (`kind = 'shadow'` in the database). The shadow carries its own holdings, because under the 20-buy/30-hold buffer the shadow's picks depend on its own previous book rather than the traded one. It has no orders, no typeable reference, and no path through the approval transitions.

The counterfactual is exact because fills are at the official closing price via MOC. That price is a single published number, so a portfolio that was not traded can be marked with zero fill assumption and zero simulation error; its return series is as real as the traded one.

Alpaca supports multiple simultaneous paper accounts, so running two live portfolios is feasible; it is simply unnecessary and worse. Two live accounts double the operational surface and then diverge for reasons unrelated to `w`: partial fills, timing skew, a submission missing the cutoff on one side. Rejected: alternating months, which halves the sample and confounds the comparison with market regime.

The validated component is the one that trades. If the shadow portfolio beats the traded one over a meaningful sample, `w` moves off zero (a new `panel_config` row through the CLI) and the roles swap. Whatever is running when live capital is authorised should be the configuration that earned it. This comparison sits alongside the paper-to-live criteria and does not gate them; it needs years to say anything.

## Panel logs

The structured output lives in Postgres, because `/why` and the UI's panel view query it: per name and per dimension, the score, the reason string, the Analyst's quotes with citations, the model ID per role, and token counts and cost.

The full prompt and response bodies and the source filing texts are content-addressed zstd files on the `/mnt/data` volume, referenced by sha256 from the database. The nightly job hash-checks a sample of referenced files, so a missing body is noticed before a replay needs it. Details in [08 — Database schema](08-database-schema.md).

## Service shape

The panel runs inside the `pipeline` service, which holds the Anthropic credentials. It has its own failure mode (external API dependency, sub-24-hour deadline) and must degrade to neutral rather than block the rebalance.

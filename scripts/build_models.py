"""Build the two shipped English bag-of-words spam models.

Run:  python3 scripts/build_models.py

The two artifacts are deliberately different models, not two copies:

* v1 -- "ngram-bow-2024": stems-free word unigrams over a 2024 vocabulary,
  hand-tuned weights, threshold 0.50, weak on crypto/parcel scams.
* v2 -- "hardened-bow-2026": larger vocabulary including crypto and
  delivery-scam terms, re-fitted weights (emphasises urgency + payment
  lures), threshold 0.62, plus different IDF smoothing.

Scores differ materially between versions for the same email, which makes
version pinning observable in the console.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.model import LinearBoWModel, tokenize  # noqa: E402

V1_TERMS = {
    # term: (base weight, document frequency pseudo-count)
    "free": (1.35, 40),
    "winner": (1.30, 16),
    "prize": (1.15, 14),
    "claim": (1.00, 22),
    "click": (0.85, 30),
    "congratulations": (1.20, 10),
    "urgent": (1.10, 26),
    "suspended": (0.95, 12),
    "verify": (0.90, 24),
    "account": (0.15, 70),
    "password": (0.80, 20),
    "refund": (0.55, 18),
    "offer": (0.35, 34),
    "discount": (0.30, 28),
    "unsubscribe": (-0.35, 26),
    "meeting": (-1.15, 44),
    "report": (-0.85, 52),
    "attached": (-0.70, 38),
    "invoice": (-0.30, 30),
    "project": (-0.80, 50),
    "team": (-0.75, 60),
    "schedule": (-0.70, 36),
    "quarterly": (-0.65, 20),
    "review": (-0.55, 40),
    "deadline": (-0.05, 22),
    "lunch": (-0.90, 24),
    "tomorrow": (-0.60, 30),
}

# v2 keeps a superset plus scam-specific terms the 2024 model never saw.
V2_EXTRA = {
    "crypto": (1.45, 18),
    "bitcoin": (1.40, 16),
    "invest": (1.05, 22),
    "double": (0.75, 14),
    "parcel": (1.15, 18),
    "delivery": (0.80, 26),
    "fee": (0.70, 24),
    "notice": (0.55, 28),
    "unlock": (0.95, 12),
    "reschedule": (0.85, 10),
    "guaranteed": (1.25, 12),
    "limited": (0.65, 22),
    "act": (0.60, 30),
    "now": (0.25, 80),
    "money": (0.85, 34),
    "fast": (0.45, 26),
    "loan": (1.10, 18),
    "credit": (0.70, 24),
    "sale": (0.40, 30),
    "risk": (0.50, 22),
    "confirm": (0.85, 20),
    "ban": (1.00, 10),
    "terminate": (1.05, 10),
    "refunded": (0.95, 8),
    "support": (-0.25, 46),
    "notes": (-0.60, 34),
    "roadmap": (-0.95, 16),
    "standup": (-0.95, 18),
    "sprint": (-0.85, 22),
    "pull": (-0.45, 20),
    "deploy": (-0.60, 18),
    "patch": (-0.50, 18),
    "customer": (-0.30, 44),
    "thanks": (-0.70, 56),
    "regards": (-0.55, 48),
    "happy": (-0.40, 30),
}

# Weights that change value between versions even for shared vocabulary.
V2_OVERRIDES = {
    "free": 1.05,
    "winner": 1.55,
    "congratulations": 1.10,
    "urgent": 1.40,
    "verify": 1.25,
    "password": 1.30,
    "offer": 0.60,
    "account": 0.35,
    "claim": 0.85,
    "invoice": 0.25,
    "deadline": 0.20,
    "meeting": -1.25,
    "report": -0.95,
    "attached": -0.80,
    "team": -0.65,
    "project": -0.90,
    "unsubscribe": -0.20,
}


def _idf(df_count: float, total_docs: float, *, smoothing: int) -> float:
    return math.log((total_docs + smoothing) / (df_count + smoothing)) + 1.0


def _build(
    version: str,
    description: str,
    terms: dict[str, tuple[float, float]],
    *,
    bias: float,
    threshold: float,
    total_docs: float,
    smoothing: int,
    weight_scale: float,
) -> LinearBoWModel:
    vocab = sorted(terms)
    weights = []
    idf = []
    for term in vocab:
        weight, df = terms[term]
        weights.append(round(weight * weight_scale, 6))
        idf.append(round(_idf(df, total_docs, smoothing=smoothing), 6))
    return LinearBoWModel(
        version=version,
        vocabulary=vocab,
        idf=idf,
        weights=weights,
        bias=bias,
        threshold=threshold,
        description=description,
    )


def main() -> None:
    v1_terms = dict(V1_TERMS)
    v2_terms = dict(V1_TERMS)
    for term, (weight, df) in V2_EXTRA.items():
        v2_terms[term] = (weight, df)
    for term, weight in V2_OVERRIDES.items():
        _old, df = v2_terms[term]
        v2_terms[term] = (weight, df)

    v1 = _build(
        "v1",
        "bow-linear 2024 baseline (unigram tf-idf; no crypto/parcel terms)",
        v1_terms,
        bias=-1.6,
        threshold=0.50,
        total_docs=1200,
        smoothing=1,
        weight_scale=0.42,
    )
    v2 = _build(
        "v2",
        "bow-linear 2026 hardened (wider vocabulary, urgency/payment weighting)",
        v2_terms,
        bias=-1.5,
        threshold=0.60,
        total_docs=2400,
        smoothing=2,
        weight_scale=0.30,
    )

    out_dir = ROOT / "models"
    out_dir.mkdir(exist_ok=True)
    for model in (v1, v2):
        path = out_dir / f"{model.version}.json"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(model.to_dict(), fh, indent=2, sort_keys=False)
            fh.write("\n")
        print(f"wrote {path} ({len(model.vocabulary)} terms, threshold={model.threshold})")

    # Sanity: same email, different scores/versions.
    probe = "Congratulations! You are a winner. Claim your free crypto prize now."
    for model in (v1, v2):
        pred = model.predict(probe)
        print(
            f"  {model.version}: score={pred.score:.4f} label={pred.label} "
            f"tokens={tokenize(probe)[:6]}..."
        )


if __name__ == "__main__":
    main()

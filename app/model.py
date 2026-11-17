"""Versioned bag-of-words linear spam classifier.

Deterministic, dependency-light inference (NumPy only) so that the same
model artifacts produce byte-identical scores on the gateway, the worker
and in tests.  Two distinct English bag-of-words models ship with the repo;
each model version pins its own vocabulary, IDF weights and linear
coefficients.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

TOKEN_RE = re.compile(r"[a-z0-9']+")

# A small, obviously distinct stopword-ish filter.  Kept identical across
# versions on purpose: the *model* differs, not the tokenizer contract.
STOPWORDS = frozenset(
    "a an and are as at be by for from has have i in is it its of on or that "
    "the this to was were will with you your".split()
)


def tokenize(text: str) -> list[str]:
    """Lowercase word tokenization for the English bag-of-words models."""
    return [tok for tok in TOKEN_RE.findall(text.lower()) if tok not in STOPWORDS]


def _json_default(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    raise TypeError(f"not JSON serializable: {type(obj)!r}")


@dataclass(frozen=True)
class Prediction:
    score: float
    label: str
    model_version: str

    def as_dict(self) -> dict:
        return {
            "score": round(float(self.score), 6),
            "label": self.label,
            "model_version": self.model_version,
        }


class LinearBoWModel:
    """Binary bag-of-words logistic model with a fixed vector vocabulary."""

    def __init__(
        self,
        version: str,
        vocabulary: list[str],
        idf: np.ndarray,
        weights: np.ndarray,
        bias: float,
        threshold: float,
        description: str = "",
    ) -> None:
        if len(vocabulary) != len(idf) or len(vocabulary) != len(weights):
            raise ValueError(f"model {version}: vocabulary/idf/weights length mismatch")
        self.version = version
        self.vocabulary = tuple(vocabulary)
        self.idf = np.asarray(idf, dtype=np.float64)
        self.weights = np.asarray(weights, dtype=np.float64)
        self.bias = float(bias)
        self.threshold = float(threshold)
        self.description = description
        self._index = {tok: i for i, tok in enumerate(self.vocabulary)}

    # -- serialization -------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "format": "bow-linear.v1",
            "version": self.version,
            "description": self.description,
            "threshold": self.threshold,
            "bias": self.bias,
            "vocabulary": list(self.vocabulary),
            "idf": self.idf.tolist(),
            "weights": self.weights.tolist(),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "LinearBoWModel":
        if payload.get("format") != "bow-linear.v1":
            raise ValueError(f"unsupported model format: {payload.get('format')!r}")
        return cls(
            version=payload["version"],
            vocabulary=payload["vocabulary"],
            idf=np.asarray(payload["idf"], dtype=np.float64),
            weights=np.asarray(payload["weights"], dtype=np.float64),
            bias=payload["bias"],
            threshold=payload.get("threshold", 0.5),
            description=payload.get("description", ""),
        )

    # -- inference -----------------------------------------------------
    def featurize(self, tokens: Iterable[str]) -> np.ndarray:
        """Sublinear tf * idf over the *frozen* vocabulary of this version."""
        counts = np.zeros(len(self.vocabulary), dtype=np.float64)
        known = 0
        total = 0
        for tok in tokens:
            total += 1
            idx = self._index.get(tok)
            if idx is not None:
                counts[idx] += 1.0
                known += 1
        if known:
            present = counts > 0
            counts[present] = 1.0 + np.log(counts[present])
        counts /= max(1.0, float(np.sqrt(max(total, 1))))
        return counts * self.idf

    def score_tokens(self, tokens: list[str]) -> float:
        margin = float(np.dot(self.featurize(tokens), self.weights) + self.bias)
        # numerically stable logistic
        if margin >= 0:
            return 1.0 / (1.0 + float(np.exp(-margin)))
        exp = float(np.exp(margin))
        return exp / (1.0 + exp)

    def predict(self, text: str) -> Prediction:
        score = self.score_tokens(tokenize(text))
        label = "spam" if score >= self.threshold else "ham"
        return Prediction(score=score, label=label, model_version=self.version)


class ModelRegistry:
    """Immutable-by-convention set of models; the active pointer is external."""

    def __init__(self, models: dict[str, LinearBoWModel], default_version: str) -> None:
        if not models:
            raise ValueError("registry needs at least one model")
        if default_version not in models:
            raise ValueError(f"default version {default_version!r} not in registry")
        self._models = dict(models)
        self.default_version = default_version

    @classmethod
    def load_dir(cls, directory: str | os.PathLike[str], default_version: str | None = None) -> "ModelRegistry":
        path = Path(directory)
        models: dict[str, LinearBoWModel] = {}
        for file in sorted(path.glob("*.json")):
            with file.open("r", encoding="utf-8") as fh:
                model = LinearBoWModel.from_dict(json.load(fh))
            if model.version in models:
                raise ValueError(f"duplicate model version {model.version!r} in {path}")
            models[model.version] = model
        if not models:
            raise ValueError(f"no model artifacts found in {path}")
        default = default_version or os.environ.get("DEFAULT_MODEL_VERSION")
        if default is None:
            default = "v1" if "v1" in models else sorted(models)[0]
        return cls(models, default if default in models else sorted(models)[0])

    @property
    def versions(self) -> list[str]:
        return sorted(self._models)

    def get(self, version: str) -> LinearBoWModel:
        try:
            return self._models[version]
        except KeyError:
            raise KeyError(f"unknown model version {version!r}; known: {self.versions}") from None

    def __contains__(self, version: object) -> bool:
        return version in self._models

"""
Review quality rules for the RAG gold table.

Flow per review:  clean_text -> text_features -> reject_reason -> quality_score
  - reject_reason : hard rules, decides eligibility (None = eligible)
  - quality_score : soft 0..1 ranking signal, only meaningful for eligible reviews
Duplicates are NOT handled here (needs the whole table) -> do text_hash/dup_rank in SQL.

pip install emoji
"""

import hashlib
import re

import emoji

SCORING_VERSION = "v1"

BBCODE = re.compile(
    r"\[/?[a-z0-9*]+(=[^\]]*)?\]", re.I
)  # [h1], [/list], [*], [url=...]
URL = re.compile(
    r"https?://|www\.|discord\.gg|bit\.ly|\b[\w-]+\.(com|net|org|gg|ru|io)\b", re.I
)
CHECKBOX = re.compile(r"[☐☑☒✅❌]")


def clean_text(text: str | None) -> str:
    """Strip Steam BBCode tags and collapse whitespace. Also used as the text to embed."""
    t = BBCODE.sub(" ", text or "")
    return re.sub(r"\s+", " ", t).strip()


def text_features(t: str) -> dict:
    """Compute features on ALREADY CLEANED text."""
    chars = [c for c in t if not c.isspace()]
    n_chars = max(len(chars), 1)
    words = re.findall(r"\w+", t.lower())
    n_words = max(len(words), 1)

    letters = sum(c.isalpha() for c in chars)
    uppers = sum(c.isupper() for c in chars)
    cased = sum(c.isalpha() and (c.isupper() or c.islower()) for c in chars)
    # count only the EXTRA chars beyond 3 ("soooooo" -> 3 extra), so stretched words aren't spam
    repeats = sum(len(m.group()) - 3 for m in re.finditer(r"(.)\1{3,}", t))

    return {
        "token_count": len(words),
        "unique_word_count": len(set(words)),
        "unique_word_ratio": len(set(words)) / n_words,
        "emoji_ratio": emoji.emoji_count(t) / n_chars,
        "alpha_ratio": letters / n_chars,
        "repeat_char_ratio": repeats / n_chars,
        "caps_ratio": (uppers / cased) if cased else None,  # None for CJK etc.
        "checkbox_count": len(CHECKBOX.findall(t)),
        "has_url": bool(URL.search(t)),
    }


def reject_reasons(f: dict, language: str, cleaned: str) -> list[str]:
    """Hard rules. Returns all reasons that apply; empty list = accepted."""
    if language != "english":
        return [
            "unsupported_language"
        ]  # the other rules don't make sense for non-English text

    reasons = []
    if f["token_count"] < 5 or len(cleaned) < 25:
        reasons.append("too_short")
    if f["has_url"]:
        reasons.append("contains_url")
    if f["alpha_ratio"] < 0.5 or f["emoji_ratio"] > 0.3:
        reasons.append("mostly_symbols")
    if f["checkbox_count"] > 3:
        reasons.append("checkbox_template")
    if f["repeat_char_ratio"] > 0.3:
        reasons.append("char_spam")
    if (f["token_count"] <= 200 and f["unique_word_ratio"] < 0.3) or (
        f["token_count"] > 200 and f["unique_word_count"] < 40
    ):
        reasons.append("repetitive")
    return reasons


def quality_score(f: dict, clean: str) -> float:
    """Soft ranking signal for ELIGIBLE reviews (hard-fail cases already removed)."""
    score = 1.0
    if f["token_count"] < 8:
        score -= 0.15  # short but allowed
    if f["repeat_char_ratio"] > 0.1:
        score -= 0.15  # "sooooo goooood"
    if f["token_count"] <= 200 and f["unique_word_ratio"] < 0.45:
        score -= 0.15  # somewhat repetitive
    if f["caps_ratio"] is not None and f["caps_ratio"] > 0.7 and f["token_count"] > 5:
        score -= 0.1  # shouting
    if f["emoji_ratio"] > 0.1:
        score -= 0.1  # emoji-heavy but still has text
    # quality_score: shorter = ranked lower, but still eligible
    if len(cleaned) < 100:
        score -= 0.2
    return round(max(score, 0.0), 3)


def score_review(review_text: str | None, language: str) -> dict:
    """One call per review -> a dict that maps straight onto gold table columns."""
    cleaned = clean_text(review_text)
    f = text_features(cleaned)
    reason = reject_reasons(f, language, cleaned)
    return {
        "clean_text": cleaned,
        **f,
        "text_hash": hashlib.md5(cleaned.lower().encode()).hexdigest(),  # dedup in SQL
        "reject_reason": reason,
        "quality_score": quality_score(f, cleaned) if reason is None else None,
        "scoring_version": SCORING_VERSION,
    }


# read the silver reviews joined with playtime and the latest game snapshot, call score_review() on each, apply the duplicate rule, and write to gold.review_rag_score

"""
Review quality rules for the RAG gold table.

Flow per review:  clean_text -> text_features -> reject_reason -> quality_score
  - reject_reason : hard rules, decides eligibility (None = eligible)
  - quality_score : soft 0..1 ranking signal, only meaningful for eligible reviews
Duplicates are NOT handled here (needs the whole table) -> do text_hash/dup_rank in SQL.

pip install emoji

-- below libaray for lanague type check and if the content is actually useful
pip install lingua-language-detector wordfreq


"""

import hashlib
import re

import emoji

from lingua import LanguageDetectorBuilder, Language
from wordfreq import zipf_frequency

SCORING_VERSION = "v1"

BBCODE = re.compile(
    r"\[/?[a-z0-9*]+(=[^\]]*)?\]", re.I
)  # [h1], [/list], [*], [url=...]
URL = re.compile(
    r"https?://|www\.|discord\.gg|bit\.ly|\b[\w-]+\.(com|net|org|gg|ru|io)\b", re.I
)
CHECKBOX = re.compile(r"[☐☑☒✅❌]")

BBCODE_BLOCK = re.compile(
    r"\[/?(h[1-6]|list|\*|quote|table|tr|td|th|hr)(=[^\]]*)?\]", re.I
)


def clean_text(text: str | None) -> str:
    """Strip Steam BBCode tags and collapse whitespace.  Use for hashing and length rules."""
    t = BBCODE.sub(" ", text or "")
    return re.sub(r"\s+", " ", t).strip()


def clean_text_for_chunking(text: str | None) -> str:
    """Strip Steam BBCode, keep paragraph breaks. This is what gets chunked/embedded."""

    t = BBCODE_BLOCK.sub("\n", text or "")
    t = BBCODE.sub(" ", t)
    t = re.sub(
        r"[^\S\n]+", " ", t
    )  # all whitespace except \n (incl. tabs, \r, non-breaking space) -> one space
    t = re.sub(r" *\n *", "\n", t)  # no spaces around line breaks
    t = re.sub(r"\n{3,}", "\n\n", t)  # max one blank line
    return t.strip()


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

    detector = LanguageDetectorBuilder.from_all_languages().build()
    conf = detector.compute_language_confidence_values(cleaned)[0]
    lang, score = conf.language.iso_code_639_1.name.lower(), conf.value
    if language != "english" or (lang != "en" and score > 0.7):
        return [
            "unsupported_language"
        ]  # the other rules don't make sense for non-English text

    reasons = []

    words = re.findall(r"[a-zA-Z']+", cleaned.lower())
    if words:
        real = sum(zipf_frequency(w, "en") > 1.5 for w in words) / len(words)
        if real < 0.5:
            reasons.append("gibberish")
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
    if len(clean) < 100:
        score -= 0.2
    return round(max(score, 0.0), 3)


def score_review(review_text: str | None, language: str) -> dict:
    """One call per review -> a dict that maps straight onto gold table columns."""
    cleaned = clean_text(review_text)
    chunking_ready = clean_text_for_chunking(review_text)
    f = text_features(cleaned)
    reason = reject_reasons(f, language, cleaned)
    return {
        "clean_text": cleaned,
        "clean_text_for_chunking": chunking_ready,
        **f,
        "text_hash": hashlib.md5(cleaned.lower().encode()).hexdigest(),  # dedup in SQL
        "reject_reason": reason,
        "quality_score": quality_score(f, cleaned) if not reason else None,
        "scoring_version": SCORING_VERSION,
    }


# read the silver reviews joined with playtime and the latest game snapshot, call score_review() on each, apply the duplicate rule, and write to gold.review_rag_score

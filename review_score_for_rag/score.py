import re
import emoji  # pip install emoji


def text_features(text: str) -> dict:
    t = text or ""
    chars = [c for c in t if not c.isspace()]
    n_chars = max(len(chars), 1)
    words = re.findall(r"\w+", t.lower())
    n_words = max(len(words), 1)

    emoji_count = emoji.emoji_count(t)
    letters = sum(c.isalpha() for c in chars)
    uppers = sum(c.isupper() for c in chars)
    cased = sum(c.isalpha() and (c.isupper() or c.islower()) for c in chars)
    repeats = sum(len(m.group()) for m in re.finditer(r"(.)\1{3,}", t))

    return {
        "token_count": len(words),
        "emoji_ratio": emoji_count / n_chars,
        "alpha_ratio": letters / n_chars,
        "repeat_char_ratio": repeats / n_chars,
        "unique_word_ratio": len(set(words)) / n_words,
        "caps_ratio": (uppers / cased) if cased else None,  # None for CJK etc.
        "has_url": bool(re.search(r"https?://|www\.", t)),
    }


def heuristic_score(f: dict) -> float:
    score = 1.0
    if f["token_count"] < 3:
        score -= 0.4
    elif f["token_count"] < 8:
        score -= 0.15
    if f["emoji_ratio"] > 0.3:
        score -= 0.4
    if f["alpha_ratio"] < 0.5:
        score -= 0.4  # ASCII art / symbols
    if f["repeat_char_ratio"] > 0.2:
        score -= 0.2
    if f["unique_word_ratio"] < 0.4 and f["token_count"] > 5:
        score -= 0.2
    if f["caps_ratio"] and f["caps_ratio"] > 0.7 and f["token_count"] > 5:
        score -= 0.1
    if f["has_url"]:
        score -= 0.3
    return max(score, 0.0)

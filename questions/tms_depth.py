"""
TMS Depth standard — criteria for theological depth in simplicity.
Defines metrics used by audit and CI.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

# Minimal lengths for TMS-level simplicity
MIN_EXPLANATION = {
    "easy": 120,      # even simple must have pastoral hook
    "medium": 150,
    "hard": 200,
    "default": 150,
}

MIN_QUESTION_LEN = 20

# Banal patterns — questions that are pure recall without theological hook
BANAL_PATTERNS = [
    "кто написал",
    "где жили",
    "сколько провинций",
    "как звали мать",
    "в каком году",
    "как называется дворец",
]

# Distractor quality: options should not be jokes or unrelated categories
JOKE_MARKERS = [
    "только шутка",
    "случайные прозвища",
    "без смысловой нагрузки",
    "торжественная формула без смысла",
]

@dataclass
class DepthIssue:
    qid: str
    kind: str
    detail: str

def audit_question(q: dict, pool_key: str = "") -> list[DepthIssue]:
    issues: list[DepthIssue] = []
    qid = str(q.get("id") or "?")
    question = str(q.get("question") or "")
    explanation = str(q.get("explanation") or "")
    options = q.get("options") or []
    correct = q.get("correct")

    # length checks
    if len(question.strip()) < MIN_QUESTION_LEN:
        issues.append(DepthIssue(qid, "short_question", f"len {len(question)}"))

    level = "default"
    if "easy" in pool_key:
        level = "easy"
    elif "medium" in pool_key:
        level = "medium"
    elif "hard" in pool_key:
        level = "hard"
    min_exp = MIN_EXPLANATION.get(level, MIN_EXPLANATION["default"])
    if len(explanation.strip()) < min_exp:
        # allow high-confidence text questions in easy to be slightly shorter if they have pastoral hook
        # but flag as depth warning
        issues.append(DepthIssue(qid, "short_explanation", f"{len(explanation)} < {min_exp} for {level}"))

    # banal detection
    q_low = question.lower()
    for pat in BANAL_PATTERNS:
        if pat in q_low:
            # check if explanation adds theological depth
            if len(explanation) < 120:
                issues.append(DepthIssue(qid, "banal_recall", f"pattern '{pat}' without deep explanation"))
            break

    # joke distractor detection
    for opt in options:
        o_low = str(opt).lower()
        for marker in JOKE_MARKERS:
            if marker in o_low:
                issues.append(DepthIssue(qid, "joke_distractor", f"option contains '{marker}'"))
    
    # correct index validity
    if not isinstance(correct, int) or not (0 <= correct < len(options)):
        issues.append(DepthIssue(qid, "bad_correct", f"correct={correct} len={len(options)}"))

    # source presence (except chapter4 which uses review_record_id)
    if pool_key != "chapter4" and not q.get("sources") and "review_record_id" not in q:
        # legacy pools may have empty sources, flag but not fail
        if q.get("claim_type") in {"greek", "history", "interpretation"}:
            issues.append(DepthIssue(qid, "missing_sources", f"claim_type {q.get('claim_type')} without sources"))

    # explanation should not introduce stronger unreviewed claim than question
    # heuristic: explanation should not contain words like "доказано", "единогласно" for contested
    if q.get("confidence") == "contested":
        strong = ["доказано", "единогласно", "бесспорно", "однозначно доказывает"]
        for s in strong:
            if s in explanation.lower():
                issues.append(DepthIssue(qid, "overclaim_contested", f"explanation uses '{s}' for contested"))

    # theological depth: for TMS level, text questions should connect to broader theology
    # if question is text high confidence, explanation should mention at least one of: Бог, Христос, спасение, надежда, святость, etc.
    if q.get("claim_type") == "text" and q.get("confidence") == "high":
        theological_keywords = [
            "бог", "бож", "христ", "спас", "надеж", "свят", "вер", "благодат",
            "церков", "евангел", "искуп", "наслед", "любов",
        ]
        if not any(k in explanation.lower() for k in theological_keywords):
            # not always required, but flag for easy pool
            if level == "easy":
                issues.append(DepthIssue(qid, "no_theological_hook", "high text without pastoral hook"))

    return issues

def audit_pool(pool: Iterable[dict], pool_key: str = "") -> list[DepthIssue]:
    all_issues: list[DepthIssue] = []
    for q in pool:
        all_issues.extend(audit_question(q, pool_key=pool_key))
    return all_issues

def depth_score(pool: Iterable[dict]) -> float:
    """Simple depth score: avg explanation length + source factor."""
    items = list(pool)
    if not items:
        return 0.0
    avg_exp = sum(len(str(q.get("explanation") or "")) for q in items) / len(items)
    avg_src = sum(len(q.get("sources") or []) for q in items) / len(items)
    # weighted
    return avg_exp * 0.7 + avg_src * 20.0

__all__ = ["audit_question", "audit_pool", "depth_score", "DepthIssue", "MIN_EXPLANATION"]

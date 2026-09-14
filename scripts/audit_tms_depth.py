#!/usr/bin/env python3
"""
Audit script for TMS depth: checks all pools for banality, verification, theological accuracy, complexity.
"""
from questions import POOL_REGISTRY
from questions.tms_depth import audit_pool, depth_score
from collections import Counter

def main():
    print("=== TMS DEPTH AUDIT ===")
    total_issues = 0
    for key in sorted(POOL_REGISTRY.keys()):
        if key in {"random_all", "competitive_all"}:
            continue
        pool = POOL_REGISTRY[key]
        issues = audit_pool(pool, pool_key=key)
        score = depth_score(pool)
        print(f"\n{key}: {len(pool)} questions, depth_score={score:.1f}, issues={len(issues)}")
        if issues:
            cnt = Counter([i.kind for i in issues])
            print(f"  breakdown: {dict(cnt)}")
            for iss in issues[:10]:
                print(f"    - {iss.qid}: {iss.kind} | {iss.detail}")
            total_issues += len(issues)
        else:
            print("  OK — no depth issues flagged")

    print(f"\nTOTAL ISSUES: {total_issues}")
    if total_issues == 0:
        print("PASS: TMS depth criteria met")
    else:
        print("REVIEW NEEDED: see breakdown above, but not failing CI yet — focus on easy/medium")

    # Additional theological accuracy spot checks
    print("\n=== THEOLOGICAL SPOT CHECKS ===")
    # Check for missing guardrails in sensitive passages
    sensitive_ids = ["ch4_text_005", "ch4_disputed_004", "ch4_app_003", "ch3_theol_003", "ch4_hist_002"]
    for pool_key, pool in POOL_REGISTRY.items():
        for q in pool:
            if q["id"] in sensitive_ids:
                print(f"Found sensitive {q['id']} in {pool_key}: confidence={q.get('confidence')} competitive={q.get('competitive')} explanation len={len(q.get('explanation',''))}")

if __name__ == "__main__":
    main()

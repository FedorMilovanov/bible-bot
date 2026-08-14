"""Immutable per-card Chapter 4 product review registry v2."""

from __future__ import annotations

import hashlib
import json
from types import MappingProxyType

from .research_handoff import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_HANDOFF_V2,
    RESEARCH_REPOSITORY,
)

_ROWS = [('ch4_text_001', 'ch4prv2_ch4_text_001_80e6ba918afa', '80e6ba918afa9a733c0625b9c2ff63eddf1240192bde07f16c8183a796b10e46', 'w3q_001', 'neutral', 'high', 'text', 'w3mcq_001', 'SAFE_TEMPLATE'), ('ch4_gr_001', 'ch4prv2_ch4_gr_001_7a87bf39c85e', '51912779c9dd408a0d39db383d3eb4c4b9b9c4c1f8865ba5b17824118bb22654', 'w3q_003', 'neutral', 'high', 'greek', 'w3mcq_002', 'SAFE_TEMPLATE'), ('ch4_text_002', 'ch4prv2_ch4_text_002_dac12365508f', 'de6c2e050a3466edd3d02fc76ca008d7eeabbc0eb854a619941667f31b38aeff', 'w3q_005', 'neutral', 'high', 'text', 'w3mcq_003', 'REJECT_AS_PRODUCT_TEMPLATE'), ('ch4_text_003', 'ch4prv2_ch4_text_003_f20fe1f7fba1', '675ac2beb5f8bb459383f6123456d5171186e7336fd5766bf020d62aa97167e0', 'w3q_008', 'neutral', 'high', 'text', 'w3mcq_004', 'SAFE_TEMPLATE'), ('ch4_gr_002', 'ch4prv2_ch4_gr_002_27449a0b3d6b', '32f96a8d4cf3a8fc0b851a43552d7112b6eb15ab6556081a9e0a9fa54355e7cb', 'w3q_011', 'neutral', 'high', 'greek', 'w3mcq_005', 'SAFE_TEMPLATE'), ('ch4_text_004', 'ch4prv2_ch4_text_004_7a60693f9960', 'e3fabddb9c5d4c2a858f4445a68718b9ff5e91a5b634666d60f6774f5ed12822', 'w3q_016', 'neutral', 'high', 'text', 'w3mcq_006', 'SAFE_TEMPLATE'), ('ch4_text_005', 'ch4prv2_ch4_text_005_f196560668fb', '26d70df7d466c4c9a02d0e5a08f48a3ccb1b83e696cb164f39ef055e5341c16c', 'w3q_018', 'neutral', 'high', 'text', 'w3mcq_007', 'SAFE_TEMPLATE'), ('ch4_ot_001', 'ch4prv2_ch4_ot_001_a50c79ff562d', '480aaff95a5cd86d9b517e34d8e260b1b2c8eab876adfd8f094d578252608139', 'w3q_019', 'neutral', 'medium', 'interpretation', 'w3mcq_008', 'NONCOMPETITIVE_ONLY'), ('ch4_text_006', 'ch4prv2_ch4_text_006_1a633b5d8707', '9b85a0ce4ca711eee12b04900c8d0daab8dd9c9afaf0eb205a1bc81e8fdac4ee', 'w3q_021', 'neutral', 'high', 'text', 'w3mcq_009', 'SAFE_TEMPLATE'), ('ch4_text_007', 'ch4prv2_ch4_text_007_21aa15da10c7', 'bbc8117114fd453689ba576f4e450ef4c3c47822298d307464cbf7e30c5b8038', 'w3q_023', 'neutral', 'high', 'text', 'w3mcq_010', 'SAFE_TEMPLATE'), ('ch4_gr_003', 'ch4prv2_ch4_gr_003_475614d1b234', 'e31fe413b0faba36106db93bd5877feda18978be5594a3e22ea091358075771e', 'w3q_029', 'neutral', 'high', 'greek', 'w3mcq_011', 'SAFE_TEMPLATE'), ('ch4_text_008', 'ch4prv2_ch4_text_008_48451cb15551', '73d3c6dd8182f7e9392924dacb915ecd11c968c5f47485778f3e21d4cc75b71a', 'w3q_030', 'neutral', 'high', 'text', 'w3mcq_012', 'SAFE_TEMPLATE'), ('ch4_text_009', 'ch4prv2_ch4_text_009_1c864b85e921', 'cdd99ce0eb2a667fb7f872429ec2509bf793fe92844081fde1d0b0e553bfbae3', 'w3q_032', 'neutral', 'high', 'text', 'w3mcq_013', 'SAFE_TEMPLATE'), ('ch4_text_010', 'ch4prv2_ch4_text_010_b821eff88965', '056ee49fd446fa9ca1e54210a7c5cc39a0b9350cc4b5a025c6ddf7496ee6c0d7', 'w3q_034', 'neutral', 'high', 'text', 'w3mcq_014', 'SAFE_TEMPLATE'), ('ch4_ot_002', 'ch4prv2_ch4_ot_002_fb61f489fa4f', 'e5153b7d69ff47a539d3fc4e8b657196be28c0987ea9f1e93f32f8f317c74f12', 'w3q_039', 'neutral', 'high', 'text', 'w3mcq_015', 'NONCOMPETITIVE_ONLY'), ('ch4_theol_001', 'ch4prv2_ch4_theol_001_81d75bc6ea50', 'f5bc71618ae03f93be4009c2c9e6b203cf313c43445fe6b50289bac68fa0d728', 'w3q_041', 'neutral', 'high', 'text', 'w3mcq_016', 'SAFE_TEMPLATE'), ('ch4_disputed_001', 'ch4prv2_ch4_disputed_001_2899e0f18de7', '9c2c7254f00e9fed7365511053b054f6cc095a8e6040cc27b35b014cae7650ba', 'w3q_004', 'neutral', 'medium', 'interpretation', 'w3mcq_033', 'NEEDS_REWRITE'), ('ch4_syn_001', 'ch4prv2_ch4_syn_001_bcb4cedc38b6', '54d0ee0313f8b1bf9aaf29489fe3a89b22880fcf3996eae1e740a21a9f0666a0', 'w3q_009', 'neutral', 'medium', 'interpretation', 'w3mcq_034', 'NONCOMPETITIVE_ONLY'), ('ch4_disputed_002', 'ch4prv2_ch4_disputed_002_fcb014fe2e70', '8d7cc98cca556948b9d7fc7fc0786a162d3776c94d3795ca6641434582eb9a9f', 'w3q_012', 'neutral', 'contested', 'interpretation', 'w3mcq_035', 'NONCOMPETITIVE_ONLY'), ('ch4_course_001', 'ch4prv2_ch4_course_001_80c8a33b3664', '3560cb1736443b9b28b27a2180ada94e46afbafaa4bbbf2100dba8a978261a81', 'w3q_013', 'project', 'contested', 'interpretation', 'w3mcq_036', 'NEEDS_REWRITE'), ('ch4_disputed_003', 'ch4prv2_ch4_disputed_003_4a2a0d0e2cb6', 'bf0d5db4be9e58754c0f66bf2231dd44ca76e479a4c8a56dce2e6c1105cfec24', 'w3q_014', 'neutral', 'contested', 'interpretation', 'w3mcq_037', 'REJECT_AS_PRODUCT_TEMPLATE'), ('ch4_course_002', 'ch4prv2_ch4_course_002_a7d037defc3c', '6b9a8f703891724437a8d8755a8a0608c8c7fa324c89e30b05acc7ba6f845434', 'w3q_017', 'project', 'medium', 'interpretation', 'w3mcq_038', 'NEEDS_REWRITE'), ('ch4_disputed_004', 'ch4prv2_ch4_disputed_004_3fdeec15fc2a', '81a8340c4e171ed9c418c92a14c9c71badb25802041a0e3295223f675ec9e91b', 'w3q_020', 'neutral', 'medium', 'interpretation', 'w3mcq_039', 'NONCOMPETITIVE_ONLY'), ('ch4_hist_001', 'ch4prv2_ch4_hist_001_53267291f181', 'f2b3eb5f2011ed7be8bae1dadf3f12d460e5881f169ef20966091e3dcb354573', 'w3q_022', 'neutral', 'medium', 'history', 'w3mcq_040', 'NEEDS_REWRITE'), ('ch4_disputed_005', 'ch4prv2_ch4_disputed_005_28157dfdbb55', 'f6717f35cbfc99d99b22f265f966fd89d47d135d0a6b47cf13b3448bbae42b01', 'w3q_025', 'neutral', 'medium', 'interpretation', 'w3mcq_041', 'NEEDS_REWRITE'), ('ch4_course_003', 'ch4prv2_ch4_course_003_5c9a14ba2778', '25b5d83b0e09082be20f5c5d9ca55a2954c15bc096d8eac5d174270a7bfb3bcd', 'w3q_027', 'project', 'medium', 'interpretation', 'w3mcq_042', 'NEEDS_REWRITE'), ('ch4_disputed_006', 'ch4prv2_ch4_disputed_006_6aba0ea2cb95', '5da78a523937bb97fd8dfb00420fcd47abd0d52839cbbbe4b3de9bd94ebdc03e', 'w3q_033', 'neutral', 'contested', 'interpretation', 'w3mcq_043', 'NEEDS_REWRITE'), ('ch4_hist_002', 'ch4prv2_ch4_hist_002_899cff312f3a', '2a30913b31b3da09fc58612a7301382dc32bf31401deb8ed535c4a5b797dc167', 'w3q_035', 'neutral', 'medium', 'history', 'w3mcq_044', 'NEEDS_REWRITE'), ('ch4_tc_001', 'ch4prv2_ch4_tc_001_2b6fdab335b1', '996ed229939661adc82e396017c44013b0f56fec9b9ee3ab5dffdecad3bd9753', 'w3q_121', 'neutral', 'high', 'text', 'w3mcq_045', 'NEEDS_REWRITE'), ('ch4_tc_002', 'ch4prv2_ch4_tc_002_b04af18fa177', '0f4f90e83c9f68083886abda4628784ae95823e12bf2b665618a065408529f9b', 'w3q_122', 'neutral', 'medium', 'interpretation', 'w3mcq_046', 'NEEDS_REWRITE'), ('ch4_ot_003', 'ch4prv2_ch4_ot_003_556ed64f27ba', '60ec662e5901edf73762c379de3f4fc57097f6351719fab6c6fcaca4970ca9bc', 'w3q_038', 'neutral', 'contested', 'interpretation', 'w3mcq_047', 'REJECT_AS_PRODUCT_TEMPLATE'), ('ch4_ot_004', 'ch4prv2_ch4_ot_004_e1e84322ba70', '4671a8092850a1492b303b0259c513f931daf4efa9d0ffdb5d0eb8c16d599126', 'w3q_040', 'neutral', 'high', 'interpretation', 'w3mcq_048', 'NONCOMPETITIVE_ONLY'), ('ch4_gr_004', 'ch4prv2_ch4_gr_004_d612568d70fc', 'a8c35fca06e5a319e671f20e2d0c2567afc146fc5a765344e9138950a4670f0d', 'w3q_002', 'neutral', 'high', 'greek', '', ''), ('ch4_text_011', 'ch4prv2_ch4_text_011_a74f241e3642', 'e2080dd73383165d2dce65cad5700e165c950b8c9457f1444d89e5a78f1d74f7', 'w3q_006', 'neutral', 'high', 'text', '', ''), ('ch4_text_012', 'ch4prv2_ch4_text_012_04d6fa5fcfee', 'b713c6ed5669d84ffa052ad0ae65d3c7a4bb9426e61862d9464a67106c838cf0', 'w3q_010', 'neutral', 'high', 'text', '', ''), ('ch4_gr_005', 'ch4prv2_ch4_gr_005_c264a78984e4', '2d65e331a6b44d969d8a9db4c0b298db73496bdf9e43ce95300f59dd100430c7', 'w3q_015', 'neutral', 'high', 'greek', '', ''), ('ch4_lex_001', 'ch4prv2_ch4_lex_001_c5f7007107e1', 'c965ce14f2526c3df24d4a2fa734a4566385567b85eb902614ad51bb554d0748', 'w3q_024', 'neutral', 'high', 'greek', '', ''), ('ch4_theol_002', 'ch4prv2_ch4_theol_002_f0958fe91e94', '2b0608938cfba7ea560f46f27ca1acd5630dfd3fcb6ce3c98cf4f60e813a04b0', 'w3q_026', 'neutral', 'medium', 'interpretation', '', ''), ('ch4_text_013', 'ch4prv2_ch4_text_013_de343753c5d4', '005ba566ddfde59ffd5a4cee62e6b55df32bab91deb53a9f55efefdfe3cc8fba', 'w3q_028', 'neutral', 'high', 'text', '', ''), ('ch4_tc_003', 'ch4prv2_ch4_tc_003_903a331d3ce8', '8f5b9be61e8a43a3d0c3977ab00589ce551d98f0f7870c79016cd8fafdb8abb8', 'w3q_031', 'neutral', 'high', 'text', '', ''), ('ch4_theol_003', 'ch4prv2_ch4_theol_003_4a3042af3eca', '1e43113750ac96c1d70ea2b09d4d7d91885a96d805bfad2de65175b1535e59c8', 'w3q_037', 'neutral', 'high', 'text', '', ''), ('ch4_app_001', 'ch4prv2_ch4_app_001_77050f91880a', '3f5ac722cccfcc3e5de4a2255ef8c305c3d9b61b202af42911a0a477cc7eedfd', 'w3q_042', 'project', 'medium', 'application', '', ''), ('ch4_syn_002', 'ch4prv2_ch4_syn_002_1b894d84fed9', '3b5c37f110318e46914d5f8498882772170bcf0ef92d228db5e57d030daf51b9', 'w3q_091', 'neutral', 'high', 'text', '', ''), ('ch4_gr_006', 'ch4prv2_ch4_gr_006_9656be87869f', 'bd5672f007267331e5de0b79f19d87a7df411980e414f7ef5599c69b334802b3', 'w3q_093', 'neutral', 'high', 'greek', '', ''), ('ch4_hist_003', 'ch4prv2_ch4_hist_003_03fe859a9125', 'ec9fd9b989ba727b52eadbf5c884db6dde60766dadd89599cd35195c0e0e8417', 'w3q_113', 'neutral', 'medium', 'history', '', ''), ('ch4_hist_004', 'ch4prv2_ch4_hist_004_1150f1da19e1', '50fd0dfa99c88737fe191067848bd6092d01a834b32c6d4bb846f99e2421ca69', 'w3q_116', 'neutral', 'contested', 'history', '', ''), ('ch4_lex_002', 'ch4prv2_ch4_lex_002_4618aaa04447', '57e5ae3161b8a879a662b378bca941dcd5341c9bd6ff4e841ba14cbe4bff8415', 'w3q_130', 'neutral', 'medium', 'greek', '', ''), ('ch4_tc_004', 'ch4prv2_ch4_tc_004_3fbdf0e27d8e', '893117c311e5149e96d07dc77fc7c1f13f839c543bb01e70f7bbd5ba82677683', 'w3q_137', 'neutral', 'high', 'text', '', ''), ('ch4_app_002', 'ch4prv2_ch4_app_002_a9c6e648d728', 'b592364ac6d01da2e375ecc14ef09292ec4948a5d01a6a34ba3bc3ba74a14286', 'w3q_097', 'project', 'medium', 'application', '', ''), ('ch4_app_003', 'ch4prv2_ch4_app_003_175900c71ab1', '645d9c7770fc4ea1966fa51d201e0daa0d33b7f7ced07c77e32153ffa75cbd32', 'w3q_100', 'project', 'medium', 'application', '', ''), ('ch4_app_004', 'ch4prv2_ch4_app_004_fb2f4bbc207e', '87f4ee1411bd5421c87309fd4e2c7aa395284f7c4a744f6b6c2863f0ff479ff5', 'w3q_102', 'project', 'medium', 'application', '', ''), ('ch4_app_005', 'ch4prv2_ch4_app_005_88e2cb86ca38', 'cded579a27adc15456a53786ed79175adf9572d72ae5226b7e1990a11c25311c', 'w3q_103', 'project', 'medium', 'application', '', '')]

_REVIEWER = MappingProxyType({
    "reviewer_id": "chapter4-product-reconciliation-v2-agent",
    "reviewer_role": "independent_product_editorial_source_review",
})

_CONTENT_READBACK = MappingProxyType({
    "stem_correct": True,
    "four_distinct_plausible_options": True,
    "one_unambiguous_best_answer": True,
    "explanation_within_supporting_evidence": True,
    "distractor_absolute_certainty_checked": True,
    "morphology_to_exegesis_laundering_absent": True,
    "project_position_not_disguised_as_neutral": True,
    "edition_flattening_absent": True,
    "named_witness_to_original_text_shortcut_absent": True,
})


def product_card_content_digest(card: dict) -> str:
    fields = (
        "id", "question", "options", "correct", "explanation", "verse",
        "domain", "claim_type", "confidence", "position", "competitive",
    )
    payload = {key: card[key] for key in fields}
    material = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _prototype_decision(prototype_id: str, classification: str) -> str:
    if not prototype_id:
        return "NO_RESEARCH_PROTOTYPE; EFFECTIVE_CLAIM_REVIEWED_DIRECTLY"
    if classification == "REJECT_AS_PRODUCT_TEMPLATE":
        return (
            "INDEPENDENT_PRODUCT_REWRITE_AFTER_REJECTED_TEMPLATE; "
            "REFERENCE_DRIFT_NOT_WAIVED"
        )
    if classification in {"NEEDS_REWRITE", "NONCOMPETITIVE_ONLY", "COURSE_POSITION_ONLY"}:
        return (
            f"INDEPENDENT_PRODUCT_REWRITE_AFTER_{classification}; "
            "PROTOTYPE_WORDING_NOT_AUTHORITY"
        )
    return "SAFE_TEMPLATE_NOT_AUTHORITY; INDEPENDENT_PRODUCT_REVIEW_COMPLETED"


def _make_record(row: tuple) -> MappingProxyType:
    (
        product_card_id,
        product_review_record_id,
        product_card_digest,
        research_claim_id,
        claimed_position,
        claimed_confidence,
        claimed_claim_type,
        prototype_id,
        prototype_classification,
    ) = row
    research = RESEARCH_HANDOFF_V2[research_claim_id]
    record = {
        "product_card_id": product_card_id,
        "product_review_record_id": product_review_record_id,
        "product_card_content_digest_sha256": product_card_digest,
        "research_repository": RESEARCH_REPOSITORY,
        "research_authority_sha": RESEARCH_AUTHORITY_SHA,
        "research_authority_digest_sha256": RESEARCH_AUTHORITY_DIGEST_SHA256,
        "research_claim_id": research_claim_id,
        "research_effective_claim_digest": research["research_effective_claim_digest"],
        "research_handoff_schema_version": RESEARCH_HANDOFF_SCHEMA_VERSION,
        "source_ids": tuple(research["source_ids"]),
        "claim_inspection_edge_ids": tuple(research["claim_inspection_edge_ids"]),
        "claimed_position": claimed_position,
        "claimed_confidence": claimed_confidence,
        "claimed_claim_type": claimed_claim_type,
        "product_safe_phrasing_reviewed": True,
        "overclaim_blacklist_checked": True,
        "reviewer": _REVIEWER,
        "review_decision": "APPROVE_NORMAL_LEARNING_ONLY",
        "content_readback": _CONTENT_READBACK,
        "prototype_review": MappingProxyType({
            "research_prototype_id": prototype_id or None,
            "research_prototype_classification": prototype_classification or None,
            "prototype_usage_decision": _prototype_decision(
                prototype_id, prototype_classification
            ),
        }),
        "ranking_considered": False,
    }
    return MappingProxyType(record)


_records = [_make_record(row) for row in _ROWS]
PRODUCT_REVIEW_REGISTRY = MappingProxyType({
    record["product_review_record_id"]: record
    for record in _records
})
PRODUCT_REVIEW_BY_CARD_ID = MappingProxyType({
    record["product_card_id"]: record
    for record in _records
})

if len(PRODUCT_REVIEW_REGISTRY) != 52 or len(PRODUCT_REVIEW_BY_CARD_ID) != 52:
    raise ValueError("Chapter 4 must expose exactly 52 immutable v2 product review records")

__all__ = [
    "PRODUCT_REVIEW_REGISTRY",
    "PRODUCT_REVIEW_BY_CARD_ID",
    "product_card_content_digest",
]

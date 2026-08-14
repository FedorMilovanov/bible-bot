from questions.chapter3.sources_crosscutting import CLAIMS, SOURCES, source_breakdown


def test_agent_e_source_control_plane():
    assert 20 <= len(SOURCES) <= 40
    assert source_breakdown() == {
        "primary": 9,
        "academic_control": 8,
        "peer_reviewed": 7,
        "conservative": 12,
    }
    assert all(source["inspection_level"] and source["limitations"] for source in SOURCES.values())


def test_agent_e_claim_matrix_is_noncompetitive():
    assert len(CLAIMS) >= 20
    assert all(claim["competitive_candidate"] is False for claim in CLAIMS)
    assert all(claim["source_ids"] and claim["limitations"] for claim in CLAIMS)

from questions.chapter3.sources_crosscutting import SOURCES


def test_agent_e_source_count():
    assert 20 <= len(SOURCES) <= 40

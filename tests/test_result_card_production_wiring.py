from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_installs_and_drains_durable_result_card_outbox():
    assert "import telegram_result_delivery_controller as result_delivery" in SOURCE
    assert "result_delivery.install_result_card_renderer(quiz)" in SOURCE
    assert "result_delivery.result_card_delivery_job" in SOURCE

    main = SOURCE.index("def main() -> None:")
    install = SOURCE.index("result_delivery.install_result_card_renderer(quiz)", main)
    build = SOURCE.index("Application.builder()", main)
    job = SOURCE.index("result_delivery.result_card_delivery_job", main)
    transport = SOURCE.index("run_telegram_application(", main)

    assert install < build < job < transport

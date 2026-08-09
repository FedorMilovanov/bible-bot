from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

import web_api


def test_uncaught_pymongo_failure_becomes_json_503():
    app = web_api.create_app()
    app.config.update(TESTING=True)

    @app.get("/api/test-db-failure")
    def db_failure():
        raise ServerSelectionTimeoutError("database unavailable")

    response = app.test_client().get("/api/test-db-failure")

    assert response.status_code == 503
    assert response.get_json() == {"error": "database temporarily unavailable"}
    assert response.headers["Cache-Control"] == "no-store"


def test_duplicate_key_keeps_specific_conflict_response():
    app = web_api.create_app()
    app.config.update(TESTING=True)

    @app.get("/api/test-duplicate")
    def duplicate():
        raise DuplicateKeyError("duplicate")

    response = app.test_client().get("/api/test-duplicate")

    assert response.status_code == 409
    assert response.get_json() == {"error": "another active quiz already exists; retry start"}

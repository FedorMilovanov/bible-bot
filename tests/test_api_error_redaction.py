from __future__ import annotations

import web_api.routes as routes


def test_route_error_boundary_does_not_reflect_internal_exception_text(monkeypatch):
    app = routes.create_app()
    monkeypatch.setattr(routes, "require_user", lambda: ({"id": 991777}, None))
    monkeypatch.setattr(
        routes,
        "start_quiz",
        lambda _user, _payload: (
            None,
            "Traceback: RuntimeError secret-token=do-not-expose",
            500,
        ),
    )

    response = app.test_client().post("/api/quiz/start", json={})

    assert response.status_code == 500
    assert response.get_json() == {"error": "internal server error"}
    assert b"secret-token" not in response.data
    assert b"Traceback" not in response.data


def test_route_error_boundary_redacts_unknown_client_error_but_keeps_allowlisted_text():
    app = routes.create_app()
    with app.app_context():
        unknown, unknown_status = routes._json_error(
            "ValueError: internal catalog implementation detail",
            400,
        )
        known, known_status = routes._json_error("invalid quiz mode", 400)

    assert unknown_status == 400
    assert unknown.get_json() == {"error": "invalid request"}
    assert known_status == 400
    assert known.get_json() == {"error": "invalid quiz mode"}

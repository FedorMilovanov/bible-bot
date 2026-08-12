"""Interpreter startup policy for the deployed service."""
from __future__ import annotations

import os

if os.getenv("APP_ENV", "").strip().lower() == "production":
    from production_logging import configure_production_logging

    configure_production_logging()

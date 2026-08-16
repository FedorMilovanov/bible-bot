"""Compatibility module name for the bot-free production quiz controller.

The former mixed controller/standalone implementation remains available in Git
history at the stacked base and is intentionally absent from the production
runtime tree. Imports of ``telegram_controller`` resolve to
``telegram_quiz_controller`` so there is one mutable module object for
result-renderer installation and focused controller integrations.
"""
from __future__ import annotations

import signal
import sys

import telegram_quiz_controller as _core

# bot.py is imported explicitly by telegram_production before this compatibility
# name is resolved. Its obsolete RAM-persistence signal handlers must not survive
# into python-telegram-bot's production transport lifecycle.
signal.signal(signal.SIGTERM, signal.SIG_DFL)
signal.signal(signal.SIGINT, signal.default_int_handler)

# Returning the exact core module object is intentional: result delivery replaces
# ``quiz._render_result`` at startup, and all focused imports must observe that
# same replacement rather than a copied re-export attribute.
sys.modules[__name__] = _core

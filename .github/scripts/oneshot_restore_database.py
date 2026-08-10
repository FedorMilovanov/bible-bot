from pathlib import Path
import subprocess

path = Path("database.py")
original = subprocess.check_output(["git", "show", "HEAD^:database.py"])
eol = b"\r\n" if b"\r\n" in original else b"\n"

old = eol.join([
    b"def create_quiz_session(user_id: int, mode: str, question_ids: list,",
    b"                        questions_data: list,",
    b"                        level_key: str = None, level_name: str = None,",
    b"                        time_limit: int = None,",
    b"                        chat_id: int = None) -> str:",
])
new = eol.join([
    b"def create_quiz_session(user_id: int, mode: str, question_ids: list,",
    b"                        questions_data: list,",
    b"                        level_key: str = None, level_name: str = None,",
    b"                        time_limit: int = None,",
    b"                        chat_id: int = None) -> str | None:",
])
if original.count(old) != 1:
    raise SystemExit(f"expected one create_quiz_session signature, got {original.count(old)}")
patched = original.replace(old, new, 1)

old_tail = eol.join([
    b"    try:",
    b"        quiz_sessions_collection.insert_one(doc)",
    b"    except Exception as e:",
    b"        logger.error(\"create_quiz_session error: %s\", e)",
    b"    return session_id",
])
new_tail = eol.join([
    b"    try:",
    b"        quiz_sessions_collection.insert_one(doc)",
    b"    except Exception as e:",
    b"        logger.error(\"create_quiz_session error: %s\", e)",
    b"        return None",
    b"    return session_id",
])
if patched.count(old_tail) != 1:
    raise SystemExit(f"expected one create_quiz_session insert tail, got {patched.count(old_tail)}")
patched = patched.replace(old_tail, new_tail, 1)

# Strong invariant: reverse only the intended two edits and require the exact
# parent bytes. This prevents accidental truncation/normalization/churn.
reversed_bytes = patched.replace(new_tail, old_tail, 1).replace(new, old, 1)
if reversed_bytes != original:
    raise SystemExit("reverse replacement did not recover parent database.py byte-for-byte")
if b"# (rest of file unchanged below this section)" in patched:
    raise SystemExit("truncated placeholder remains in database.py")

path.write_bytes(patched)

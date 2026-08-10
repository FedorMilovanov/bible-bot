from pathlib import Path

path = Path("database.py")
data = path.read_bytes()
eol = b"\r\n" if b"\r\n" in data else b"\n"

start = b"def create_quiz_session(user_id: int, mode: str, question_ids: list,"
end = b"def get_active_quiz_session(user_id: int):"
if data.count(start) != 1 or data.count(end) != 1:
    raise SystemExit("create_quiz_session region markers are not unique")
start_i = data.index(start)
end_i = data.index(end, start_i)
region = data[start_i:end_i]
old = b"    if quiz_sessions_collection is None:" + eol + b"        return \"\""
new = b"    if quiz_sessions_collection is None:" + eol + b"        return None"
if region.count(old) != 1:
    raise SystemExit(f"expected one disabled-store empty-string return, got {region.count(old)}")
patched_region = region.replace(old, new, 1)
patched = data[:start_i] + patched_region + data[end_i:]

if patched.replace(new, old, 1) != data:
    raise SystemExit("reverse replacement did not reproduce database.py byte-for-byte")
path.write_bytes(patched)

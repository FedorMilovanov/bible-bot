from pathlib import Path

path = Path("database.py")
data = path.read_bytes()
eol = b"\r\n" if b"\r\n" in data else b"\n"

old_levels = eol.join([
    b'    "intro1", "intro2", "intro3",',
    b'    "random20", "hardcore20",',
])
new_levels = eol.join([
    b'    "intro1", "intro2", "intro3",',
    b'    "random_all", "random20", "hardcore20",',
])
if data.count(old_levels) != 1:
    raise SystemExit(f"expected one ALL_LEVEL_KEYS tail, got {data.count(old_levels)}")
patched = data.replace(old_levels, new_levels, 1)

old_points = eol.join([
    b'    "nero": 2, "geography": 2,',
    b'    "random20": 1, "hardcore20": 2,',
])
new_points = eol.join([
    b'    "nero": 2, "geography": 2,',
    b'    "random_all": 1, "random20": 1, "hardcore20": 2,',
])
if patched.count(old_points) != 1:
    raise SystemExit(f"expected one POINTS_PER_QUESTION tail, got {patched.count(old_points)}")
patched = patched.replace(old_points, new_points, 1)

if patched.replace(new_points, old_points, 1).replace(new_levels, old_levels, 1) != data:
    raise SystemExit("reverse replacement did not reproduce database.py byte-for-byte")
path.write_bytes(patched)

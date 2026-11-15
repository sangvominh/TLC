import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
root = Path(__file__).resolve().parent
label, *command = sys.argv[1:]
started = time.time()
env = dict(os.environ, DOCKER_CONTEXT="desktop-linux", PYTHONIOENCODING="utf-8")
log = root / f"{label}.log"
with log.open("wb") as output:
    completed = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, env=env)
result = {
    "command": command,
    "exit_code": completed.returncode,
    "elapsed_seconds": round(time.time() - started, 3),
    "log": str(log),
}
(root / f"{label}.result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result), flush=True)
print("\n".join(log.read_text(encoding="utf-8", errors="replace").splitlines()[-35:]))
sys.exit(completed.returncode)

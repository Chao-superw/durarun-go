"""Exercise the public runner and example with a real process interruption."""

import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


with tempfile.TemporaryDirectory(prefix="durarun-smoke-") as tmp:
    root = Path(tmp)
    runner, agent, state = (root / name for name in ("runner", "agent", "state"))
    subprocess.run(["go", "build", "-o", runner, "./cmd/durarun-local"], check=True)
    subprocess.run(["go", "build", "-o", agent, "./examples/sdk-agent"], check=True)

    log = root / "first.log"
    with log.open("w") as output:
        first = subprocess.Popen(
            [runner, "run", "-state", state, "--", agent, "inspect source"],
            env={**os.environ, "SDK_EXAMPLE_PAUSE": "1"},
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 15
            while "checkpoint committed" not in log.read_text():
                if first.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(f"Checkpoint not committed:\n{log.read_text()}")
                time.sleep(0.05)
            first.send_signal(signal.SIGINT)
            if first.wait(timeout=10) != 130:
                raise RuntimeError(f"Unexpected interruption status:\n{log.read_text()}")
        finally:
            if first.poll() is None:
                first.send_signal(signal.SIGTERM)
                first.wait(timeout=10)

    resumed = subprocess.run(
        [runner, "resume", "-state", state], capture_output=True, text=True, timeout=15
    )
    if resumed.returncode:
        raise RuntimeError(f"Resume failed:\n{resumed.stdout}\n{resumed.stderr}")
    if "inspecting input" in resumed.stdout:
        raise RuntimeError("Resume ran a step that was already committed.")
    workspace = next(
        line.removeprefix("workspace: ")
        for line in resumed.stderr.splitlines()
        if line.startswith("workspace: ")
    )
    if (Path(workspace) / "report.md").read_text() != "# Inspection\n\ninspect\nsource\n":
        raise RuntimeError("The resumed report differs from the expected output.")
    print("Checkpoint restored. The completed step was skipped and the report matches.")

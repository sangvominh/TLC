# Intentional preflight failure test

This folder was created by an isolated launcher test that simulated an unavailable HTTP endpoint. The expected result was runner status `failed` with exit code 1, zero Docker invocations, no report-copy attempt and no cleaning submission. This is not a failure of the real cluster or a cleaning run. The verified `latest.json` was not changed.

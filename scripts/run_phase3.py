"""Run Phase 3 in Docker; host needs only Python standard library and Docker CLI."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import uuid

ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Run and export the five Phase 3 KPIs.")
    parser.add_argument("--docker-context", default=os.environ.get("DOCKER_CONTEXT"))
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()
    project = Path(__file__).resolve().parent.parent
    run_id = "p3-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    report_dir = project / "reports/phase3" / run_id
    output_dir = project / "results/phase3" / run_id
    report_dir.mkdir(parents=True)
    docker = ["docker"] + (["--context", args.docker_context] if args.docker_context else [])
    started = time.perf_counter()
    timings = {}

    def run(command, label, capture=False):
        begin = time.perf_counter()
        print(f"[{label}] running", flush=True)
        process = subprocess.run(command, cwd=project, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (report_dir / (label + ".log")).write_bytes(process.stdout + b"\nSTDERR:\n" + process.stderr)
        timings[label] = {"seconds": round(time.perf_counter() - begin, 3), "exit_code": process.returncode}
        if process.returncode:
            raise RuntimeError(label + " failed:\n" + (process.stdout + process.stderr).decode("utf-8", errors="replace")[-5000:])
        return process.stdout.decode("utf-8") if capture else None

    try:
        print("Phase 3 run:", run_id, flush=True)
        run([*docker, "info", "--format", "{{.ServerVersion}}"], "docker-info")
        # Fail early if the cluster is not running; never recreate Phase 2 or erase HDFS here.
        readiness_code = (
            "import json,urllib.request; "
            "get=lambda u:json.load(urllib.request.urlopen(u,timeout=5)); "
            "m=get('http://spark-master:8080/json/'); "
            "d=get('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState')['beans'][0]; "
            "n=get('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo')['beans'][0]; "
            "assert m['status']=='ALIVE' and any(w['state']=='ALIVE' for w in m['workers']) and d['NumLiveDataNodes']>0 and n['Safemode']=='', 'Cluster not ready'; print('READY')"
        )
        deadline = time.monotonic() + 120
        while True:
            try:
                run([*docker, "exec", "spark-master", "python3", "-c", readiness_code], "readiness")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Cluster not ready after 120 seconds. Start Spark/HDFS first; see README.")
                time.sleep(3)
        reference = project / "data/reference"
        reference.mkdir(parents=True, exist_ok=True)
        zone_file = reference / "taxi_zone_lookup.csv"
        if not zone_file.exists():
            content = urllib.request.urlopen(ZONE_URL, timeout=30).read()
            parsed = list(csv.DictReader(io.StringIO(content.decode("utf-8-sig"))))
            if not parsed or not {"LocationID", "Borough", "Zone", "service_zone"}.issubset(parsed[0]):
                raise ValueError("Official zone download is not the expected CSV")
            zone_file.write_bytes(content)
        zone_manifest = {"url": ZONE_URL, "sha256": hashlib.sha256(zone_file.read_bytes()).hexdigest(),
                         "bytes": zone_file.stat().st_size}
        (reference / "manifest.json").write_text(json.dumps(zone_manifest, indent=2), encoding="utf-8")
        run([*docker, "exec", "-u", "0", "spark-master", "mkdir", "-p", "/opt/spark/work-dir/sql/phase3", "/opt/spark/phase3-tests", "/data/reference", "/data/analytics", "/data/analytics_reports"], "prepare-dirs")
        copies = [
            (str(project / "src/analyze.py"), "spark-master:/opt/spark/work-dir/analyze.py"),
            (str(project / "src/sql/phase3") + "/.", "spark-master:/opt/spark/work-dir/sql/phase3/"),
            (str(project / "tests/test_analytics.py"), "spark-master:/opt/spark/phase3-tests/test_analytics.py"),
            (str(zone_file), "spark-master:/data/reference/taxi_zone_lookup.csv"),
        ]
        for index, (source, target) in enumerate(copies):
            run([*docker, "cp", source, target], f"copy-{index}")
        run([*docker, "exec", "-u", "0", "spark-master", "chmod", "a+rwx", "/data/analytics", "/data/analytics_reports"], "output-permissions")
        source_files = [project / "src/analyze.py", project / "tests/test_analytics.py", Path(__file__),
                        project / "run.py", *sorted((project / "src/sql/phase3").glob("*.sql"))]
        hashes = {str(p.relative_to(project)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
        (report_dir / "source-hashes.json").write_text(json.dumps({"files": hashes, "zones": zone_manifest}, indent=2), encoding="utf-8")
        if not args.skip_tests:
            run([*docker, "exec", "spark-master", "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "768m", "--py-files", "/opt/spark/work-dir/analyze.py", "/opt/spark/phase3-tests/test_analytics.py"], "synthetic-tests")
        run([*docker, "exec", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/opt/spark/work-dir/analyze.py", "--run-id", run_id], "spark-analysis")
        run([*docker, "cp", f"spark-master:/data/analytics_reports/{run_id}/.", str(report_dir)], "export-reports")
        report = json.loads((report_dir / "analysis_report.json").read_text(encoding="utf-8"))
        if report["status"] != "passed" or len(report["outputs"]) != 5 or not all(report["checks"].values()):
            raise ValueError("Spark report did not pass all checks")
        output_dir.mkdir(parents=True)
        manifest = []
        for output in report["outputs"]:
            filename = output["filename"]
            if not filename.endswith(".csv") or Path(filename).name != filename:
                raise ValueError("Unsafe CSV filename")
            target = output_dir / filename
            run([*docker, "cp", f"spark-master:/data/analytics/{run_id}/{filename}", str(target)], "export-" + filename[:-4])
            with target.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != output["columns"] or sum(1 for _ in reader) != output["rows"]:
                    raise ValueError("Host CSV readback mismatch: " + filename)
            manifest.append({"filename": filename, "rows": output["rows"], "bytes": target.stat().st_size,
                             "sha256": hashlib.sha256(target.read_bytes()).hexdigest()})
        master_code = "import json,urllib.request; print(json.dumps(json.load(urllib.request.urlopen('http://spark-master:8080/json/',timeout=10))))"
        for attempt in range(6):
            master = json.loads(run([*docker, "exec", "spark-master", "python3", "-c", master_code], "spark-master", capture=True))
            matches = [app for app in master.get("completedapps", []) if app["id"] == report["application_id"]]
            if matches and matches[0]["state"] == "FINISHED":
                break
            time.sleep(2)
        else:
            raise ValueError("Spark Master has not confirmed FINISHED")
        (report_dir / "spark-master.json").write_text(json.dumps(master, indent=2), encoding="utf-8")
        (report_dir / "export-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        status = {"status": "passed", "run_id": run_id, "input_rows": report["profile"]["input_rows"],
                  "tests_run": not args.skip_tests, "timings": timings,
                  "elapsed_seconds": round(time.perf_counter() - started, 3),
                  "results": str(output_dir.relative_to(project)), "reports": str(report_dir.relative_to(project)),
                  "source_phase2_generated_at": report["source_phase2_report"]["generated_at_utc"]}
        (report_dir / "runner-status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
        (report_dir.parent / "latest.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
        print("PHASE 3 PASSED:", json.dumps(status), flush=True)
    except Exception as error:
        (report_dir / "runner-status.json").write_text(json.dumps({"status": "failed", "run_id": run_id, "error": str(error), "timings": timings}, indent=2), encoding="utf-8")
        print("FAILED:", error, "\nLogs:", report_dir, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

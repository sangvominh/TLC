import json
from pathlib import Path
import subprocess
import time
import urllib.request

root = Path(__file__).resolve().parent
project = root.parent.parent
docker = ["docker", "--context", "desktop-linux"]
commands = []
for container in ("namenode", "datanode"):
    for filename in ("core-site.xml", "hdfs-site.xml"):
        commands.append([*docker, "cp", str(project / "config" / filename),
                         f"{container}:/opt/hadoop/etc/hadoop/{filename}"])
for container in ("spark-master", "spark-worker"):
    commands.append([*docker, "cp", str(project / "config"), f"{container}:/opt/hadoop-conf"])
commands.extend([
    [*docker, "cp", str(project / "src") + "/.", "spark-master:/opt/spark/work-dir/"],
    [*docker, "cp", str(project / "tests") + "/.", "spark-master:/opt/spark/tests/"],
    [*docker, "cp", str(project / "scripts"), "namenode:/scripts"],
    [*docker, "cp", str(project / "data" / "raw"), "spark-master:/data/"],
    [*docker, "compose", "--project-name", "bigdata", "-f", "docker-compose.yml",
     "-f", ".tools/runtime-main-20261114/compose.runtime.yml", "start"],
    [*docker, "exec", "-u", "0", "spark-master", "chmod", "-R", "a+rwX", "/data"],
])
with (root / "prepare.log").open("wb") as log:
    for command in commands:
        print("Running:", " ".join(command), flush=True)
        subprocess.run(command, cwd=project, stdout=log, stderr=subprocess.STDOUT, check=True)

deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen("http://localhost:8080/json/", timeout=3) as response:
            master = json.load(response)
        with urllib.request.urlopen("http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=3) as response:
            filesystem = json.load(response)["beans"][0]
        with urllib.request.urlopen("http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", timeout=3) as response:
            namenode = json.load(response)["beans"][0]
        state = {"master": master.get("status"),
                 "alive_workers": sum(worker.get("state") == "ALIVE" for worker in master.get("workers", [])),
                 "live_datanodes": filesystem.get("NumLiveDataNodes", 0),
                 "safemode": namenode.get("Safemode")}
        print(json.dumps(state), flush=True)
        if (state["master"] == "ALIVE" and state["alive_workers"] > 0
                and state["live_datanodes"] > 0 and state["safemode"] == ""):
            (root / "readiness.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
            break
    except Exception as error:
        print(type(error).__name__, str(error), flush=True)
    time.sleep(3)
else:
    raise RuntimeError("Cluster did not become ready within 120 seconds")

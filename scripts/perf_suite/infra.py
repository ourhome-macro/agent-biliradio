from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import requests

ROOT = Path(__file__).resolve().parents[2]


def port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


class Minio:
    def __init__(self, directory):
        self.port, self.console = port(), port()
        self.user, self.secret = "perf-" + uuid4().hex[:12], uuid4().hex + uuid4().hex
        directory = Path(directory)
        directory.mkdir(parents=True)
        self.log = (directory / "minio.log").open("wb")
        self.process = subprocess.Popen(
            [
                shutil.which("minio") or str(ROOT / "minio.exe"),
                "server",
                str(directory / "objects"),
                "--address",
                f"127.0.0.1:{self.port}",
                "--console-address",
                f"127.0.0.1:{self.console}",
            ],
            env={**os.environ, "MINIO_ROOT_USER": self.user, "MINIO_ROOT_PASSWORD": self.secret},
            stdout=self.log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.origin = f"http://127.0.0.1:{self.port}"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if requests.get(self.origin + "/minio/health/live", timeout=0.3).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        self.close()
        raise RuntimeError("Isolated MinIO startup failed")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()


class Kafka:
    def __init__(self):
        from confluent_kafka.admin import AdminClient

        self.port = port()
        self.run_id = uuid4().hex
        self.name = "radio-perf-" + self.run_id[:12]
        self.bootstrap = f"127.0.0.1:{self.port}"
        cluster = base64.urlsafe_b64encode(uuid4().bytes).decode().rstrip("=")
        env = {
            "CLUSTER_ID": cluster,
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://localhost:9092,EXTERNAL://127.0.0.1:{self.port}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
            ),
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
            "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
        }
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--memory",
            "1g",
            "--cpus",
            "2",
            "--label",
            "radio.perf.run=" + self.run_id,
            "--publish",
            f"127.0.0.1:{self.port}:9094",
        ]
        for key, value in env.items():
            command += ["--env", key + "=" + value]
        subprocess.run([*command, "apache/kafka:4.0.1"], check=True, capture_output=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if (
                    AdminClient({"bootstrap.servers": self.bootstrap, "log_level": 0})
                    .list_topics(timeout=1)
                    .brokers
                ):
                    return
            except Exception:
                time.sleep(0.5)
        self.close()
        raise RuntimeError("Isolated Kafka startup failed")

    def close(self):
        inspected = subprocess.run(
            ["docker", "inspect", self.name], capture_output=True, check=False
        )
        if inspected.returncode == 0:
            data = json.loads(inspected.stdout)[0]
            if data["Config"]["Labels"].get("radio.perf.run") != self.run_id:
                raise RuntimeError("Refusing to remove a container not owned by this run")
            subprocess.run(["docker", "rm", "--force", self.name], check=True, capture_output=True)

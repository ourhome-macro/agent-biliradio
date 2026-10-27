"""Own a private native MySQL test instance; never discover or modify system services."""
from __future__ import annotations

import os
import secrets
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import pymysql
from sqlalchemy import URL


class SecretURL(str):
    def __repr__(self):
        from sqlalchemy.engine import make_url
        return repr(make_url(self).render_as_string(hide_password=True))


class NativeMySQL:
    def __init__(self, directory, *, binary=None):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self.external = None
        configured_admin = os.getenv("RADIO_TEST_MYSQL_ADMIN_URL")
        if configured_admin:
            from sqlalchemy.engine import make_url

            target = make_url(configured_admin)
            if target.host not in {"127.0.0.1", "localhost"} or target.port in {None, 3306, 3307}:
                raise ValueError("Integration MySQL must be an explicit isolated loopback test port")
            self.external = pymysql.connect(
                host=target.host,
                port=target.port,
                user=target.username,
                password=target.password,
                autocommit=True,
            )
            self.schema = "radio_native_" + uuid4().hex
            with self.external.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE `{self.schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
                )
            self.url = SecretURL(
                target.set(database=self.schema).render_as_string(hide_password=False)
            )
            return
        configured = binary or os.getenv("MYSQLD_PATH")
        self.binary = Path(configured) if configured else Path(tempfile.gettempdir()) / "radio-mysql-bin/mysql-8.4.9-winx64/bin/mysqld.exe"
        if not self.binary.is_file():
            raise FileNotFoundError("Set MYSQLD_PATH to a native MySQL 8.4+ mysqld executable")
        with socket.socket() as stream:
            stream.bind(("127.0.0.1", 0))
            self.port = stream.getsockname()[1]
        self.log = (self.directory / "mysql.log").open("wb")
        self.datadir = self.directory / "data"
        common = [str(self.binary), "--no-defaults", f"--basedir={self.binary.parent.parent}",
                  f"--datadir={self.datadir}"]
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        result = subprocess.run(common + ["--initialize-insecure"], stdout=self.log,
                                stderr=subprocess.STDOUT, timeout=60, creationflags=flags)
        if result.returncode:
            self.log.close()
            raise RuntimeError("Isolated MySQL initialization failed; inspect fixture mysql.log")
        self.process = subprocess.Popen(common + [f"--port={self.port}", "--bind-address=127.0.0.1",
            "--mysqlx=OFF", "--innodb-buffer-pool-size=64M", "--max-connections=80", "--console"],
            stdout=self.log, stderr=subprocess.STDOUT, creationflags=flags)
        self.password = secrets.token_urlsafe(24)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.close()
                raise RuntimeError("Isolated MySQL stopped before readiness; inspect fixture mysql.log")
            try:
                conn = pymysql.connect(host="127.0.0.1", port=self.port, user="root", connect_timeout=1)
                with conn.cursor() as cursor:
                    cursor.execute("ALTER USER 'root'@'localhost' IDENTIFIED BY %s", (self.password,))
                    cursor.execute("CREATE DATABASE radio_acceptance CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
                conn.close()
                self.url = SecretURL(URL.create("mysql+pymysql", username="root", password=self.password,
                    host="127.0.0.1", port=self.port, database="radio_acceptance").render_as_string(hide_password=False))
                return
            except pymysql.err.OperationalError:
                time.sleep(.1)
        self.close()
        raise TimeoutError("Isolated MySQL startup timed out")

    def close(self):
        if self._closed:
            return
        if self.external is not None:
            if not self.schema.startswith("radio_native_") or not self.schema[13:].isalnum():
                raise RuntimeError("Refusing to clean an unowned test schema")
            with self.external.cursor() as cursor:
                cursor.execute(f"DROP DATABASE `{self.schema}`")
            self.external.close()
            self.external = None
            self._closed = True
            return
        if getattr(self, "process", None) and self.process.poll() is None:
            try:
                connection = pymysql.connect(host="127.0.0.1", port=self.port, user="root",
                                             password=self.password, connect_timeout=1)
                connection.cursor().execute("SHUTDOWN")
                connection.close()
                self.process.wait(timeout=10)
            except Exception:
                self.process.terminate()
                self.process.wait(timeout=10)
        self.log.close()
        self._closed = True

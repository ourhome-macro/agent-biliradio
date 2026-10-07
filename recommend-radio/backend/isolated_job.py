"""Bound heavy Stream deliveries in a process and terminate their subprocess tree."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

from job_errors import JobPermanentFailure, JobReconciliationRequired


class ProcessTree:
    def __init__(self, process):
        self.process = process
        self.handle = None
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            class Basic(ctypes.Structure):
                _fields_ = [
                    ("ProcessTime", ctypes.c_longlong),
                    ("JobTime", ctypes.c_longlong),
                    ("Flags", wintypes.DWORD),
                    ("MinWork", ctypes.c_size_t),
                    ("MaxWork", ctypes.c_size_t),
                    ("Active", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("Priority", wintypes.DWORD),
                    ("Scheduling", wintypes.DWORD),
                ]

            class IO(ctypes.Structure):
                _fields_ = [
                    (name, ctypes.c_ulonglong)
                    for name in (
                        "Reads",
                        "Writes",
                        "Others",
                        "ReadBytes",
                        "WriteBytes",
                        "OtherBytes",
                    )
                ]

            class Extended(ctypes.Structure):
                _fields_ = [
                    ("Basic", Basic),
                    ("IO", IO),
                    ("ProcessMemory", ctypes.c_size_t),
                    ("JobMemory", ctypes.c_size_t),
                    ("PeakProcess", ctypes.c_size_t),
                    ("PeakJob", ctypes.c_size_t),
                ]

            self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            self.kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            self.kernel.CreateJobObjectW.restype = wintypes.HANDLE
            self.kernel.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
            ]
            self.kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            self.kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            handle = self.kernel.CreateJobObjectW(None, None)
            info = Extended()
            info.Basic.Flags = 0x2000  # KILL_ON_JOB_CLOSE
            if not handle or not self.kernel.SetInformationJobObject(
                handle, 9, ctypes.byref(info), ctypes.sizeof(info)
            ):
                process.kill()
                if handle:
                    self.kernel.CloseHandle(handle)
                raise OSError("Could not establish Windows process-tree containment")
            if not self.kernel.AssignProcessToJobObject(handle, int(process._handle)):
                process.kill()
                self.kernel.CloseHandle(handle)
                raise OSError("Could not contain the job process")
            self.handle = handle

    def terminate(self):
        if self.handle:
            self.kernel.TerminateJobObject(self.handle, 1)
        elif self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def dispatch_isolated(job):
    limit = 3660 if job["kind"] == "media_import" else 270
    process = subprocess.Popen(
        [sys.executable, "-m", "isolated_job"],
        cwd=Path(__file__).resolve().parent,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
    tree = ProcessTree(process)
    try:
        try:
            output, _ = process.communicate(json.dumps(job, ensure_ascii=False), timeout=limit)
        except subprocess.TimeoutExpired:
            tree.terminate()
            process.communicate(timeout=10)
            raise TimeoutError("DurableJobExecutionDeadlineExceeded") from None
        if process.returncode:
            raise RuntimeError("IsolatedJobProcessFailed")
        envelope = json.loads(output.splitlines()[-1])
        if "error" in envelope:
            category = envelope["error"]
            error = (
                JobPermanentFailure
                if category == "permanent"
                else JobReconciliationRequired
                if category == "reconciliation"
                else RuntimeError
            )
            raise error(envelope["errorType"])
        return envelope["result"]
    finally:
        try:
            if process.poll() is None:
                tree.terminate()
                process.wait(timeout=10)
        finally:
            tree.close()


def main():
    job = json.loads(sys.stdin.read())
    from music_agent import current_job_id
    from task_app import dispatch
    from telemetry_setup import setup

    from agent_memory_runtime.telemetry import flush, span

    setup("radio-job-process")
    token = current_job_id.set(job["job_id"])
    try:
        with span(
            "task.isolated.execute",
            parent=json.loads(job["trace_context"]),
            attributes={"job.id": job["job_id"], "job.kind": job["kind"]},
        ):
            result = dispatch(job)
        envelope = {"result": result}
    except Exception as error:
        category = (
            "permanent"
            if isinstance(error, JobPermanentFailure)
            else "reconciliation"
            if isinstance(error, JobReconciliationRequired)
            else "retry"
        )
        envelope = {"error": category, "errorType": type(error).__name__}
    finally:
        current_job_id.reset(token)
        flush()
    print(json.dumps(envelope, ensure_ascii=False))


if __name__ == "__main__":
    main()

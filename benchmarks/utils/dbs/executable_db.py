from abc import ABC, abstractmethod
from io import TextIOWrapper
import logging
from pathlib import Path
import re

import pandas as pd
import rdflib
from rdflib import Literal, URIRef
import traceback
from rdflib.namespace import XSD
from rdflib.query import Result
from rdflib.term import Node
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from ..datasets.base_dataset import BaseDataset
from .base_db import BaseDB
from .stats.ps import PSStatRecorder
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExecutableDB(BaseDB):
    def __init__(
        self,
        id: str,
        port_id: int,
        dataset: BaseDataset,
        endpoint: str | None = None,
        logger_dir=Path("./logs"),
        name: str = __name__,
        base_dir: Path | None = None,
        db_dir: Path | None = None,
        use_encoded_ttl: bool = False,
        kill_existing: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(
            id=id,
            port_id=port_id,
            dataset=dataset,
            endpoint=endpoint,
            logger_dir=logger_dir,
            name=name,
            use_encoded_ttl=use_encoded_ttl,
            *args,
            **kwargs,
        )
        self.base_dir = base_dir if base_dir is not None else Path("./scratch") / id
        self.db_dir = db_dir if db_dir is not None else self.base_dir / "db" / id
        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True, exist_ok=True)
        self.server_log_file = self.db_dir / f"{self.id}_run.log"
        self.server_log_file_fd: TextIOWrapper = self.server_log_file.open("a")
        self.clear_log()
        self.server: subprocess.Popen | None = None
        self.pid: int | None = None
        self.enable_kill_existing = kill_existing
        self.kill_existing_processes()

    @abstractmethod
    def start_request(self):
        pass

    def kill_existing_processes(self):
        if self.enable_kill_existing:
            logger.warning(
                f"Killing any existing process using port {self.port_id} before starting the server"
            )
            self.run_command(f"fuser -k {self.port_id}/tcp", allow_fail=True)

    def read_log(self) -> str:
        if self.server_log_file.exists():
            with open(self.server_log_file, "r") as f:
                return f.read()
        else:
            return ""

    def clear_log(self):
        if self.server_log_file_fd is not None and not self.server_log_file_fd.closed:
            self.server_log_file_fd.close()
        if self.server_log_file.exists():
            self.server_log_file.unlink()
        self.server_log_file_fd = open(self.server_log_file, "a")

    def get_timings(self, key: str):
        log = self.read_log()
        timings = []
        matches = re.finditer(rf"--TIMING \({key}\) END: ([0-9,]+) ns--", log)
        for match in matches:
            timings.append(int(match.group(1)))
        return timings

    def get_timings_per_query_key(self, key: str):
        log = self.read_log()
        per_query_timings = []
        current_query_timings = None
        lines = log.splitlines()
        for line in lines:
            if self.start_request() in line:
                if current_query_timings is not None:
                    per_query_timings.append(current_query_timings)
                current_query_timings = []
            elif f"--TIMING ({key}) END:" in line:
                match = re.search(r"--TIMING \({key}\) END: ([0-9,]+) ns--", line)
                if match:
                    current_query_timings.append(int(re.sub(r",", "", match.group(1))))
        if current_query_timings is not None:
            per_query_timings.append(current_query_timings)
        return per_query_timings

    def get_timings_per_query(self):
        log = self.read_log()
        per_query_timings: list[dict[str, list[int]]] = []
        current_query_timings: dict[str, list[int]] = None
        lines = log.splitlines()
        for line in lines:
            if self.start_request() in line:
                if current_query_timings is not None:
                    per_query_timings.append(current_query_timings)
                current_query_timings = {}
            match = re.search(r"--TIMING \((.+)\) END: ([0-9,]+) +ns--", line)
            if match:
                key = match.group(1)
                timing = int(re.sub(r",", "", match.group(2)))
                if current_query_timings is not None:
                    if key not in current_query_timings:
                        current_query_timings[key] = []
                    current_query_timings[key].append(timing)
        if current_query_timings is not None:
            per_query_timings.append(current_query_timings)
        return per_query_timings

    @abstractmethod
    def setup(self):
        pass

    def stop(self):
        logger.info("Stopping server!")
        if self.server is not None:
            self.server.terminate()
            self.server = None
        if self.server_log_file_fd is not None:
            self.server_log_file_fd.close()
        # find associated port and process and kill it if still running
        # this is a fallback in case the server process was not properly terminated and is still running
        self.run_command(f"fuser -k {self.port_id}/tcp", allow_fail=True)
        self.pid = None

    def start_record_stats(self):
        if self.pid is None:
            raise RuntimeError(
                "Cannot start recording stats: server process not running / PID not set"
            )
        self.stat_recorder = PSStatRecorder(self.pid)
        self.stat_recorder.clear_stats()
        self.stat_recorder.start_recording()

    def stop_record_stats(self):
        self.stat_recorder.stop_recording()

    def get_stats(self) -> pd.DataFrame:
        return self.stat_recorder.get_stats()

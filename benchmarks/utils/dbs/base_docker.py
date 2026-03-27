from abc import ABC, abstractmethod
from calendar import c
from io import TextIOWrapper
import json
import logging
from math import log
from pathlib import Path
import re
import threading

from numpy import number, power
import pandas as pd
import rdflib
from rdflib import Literal, URIRef
import traceback
from rdflib.namespace import XSD
from rdflib.query import Result
from rdflib.term import Node
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from test.test_reprlib import r
from ..datasets.base_dataset import BaseDataset
from .base_db import BaseDB

import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StatRecorder:
    def __init__(self, container_name: str):
        self.container_name = container_name
        self.stats_map = {
            "cpu_percent": "CPUPerc",
            "mem_usage": "MemUsage",
            "net_io": "NetIO",
            "block_io": "BlockIO",
            "pids": "PIDs",
        }
        self.stats_parsers = {
            "cpu_percent": lambda x: float(x.strip("%")),
            "mem_usage": lambda x: self.parse_datasize(x),
            "net_io": lambda x: self.parse_datasize(x),
            "block_io": lambda x: self.parse_datasize(x),
            "pids": lambda x: int(x),
        }
        logger.debug(
            f"Initialized StatRecorder for container {self.container_name} with stats_map: {self.stats_map} and stats_parsers: {self.stats_parsers}"
        )
        self.stats_lock = threading.RLock()
        self.clear_stats()

    def parse_datasize(self, size_str: str) -> float:
        # size_str is in the format "123.45MB" or "1.23GB"

        used_str, total_str = size_str.split("/")

        try:
            size_str = used_str.strip().upper()
            multiplier = 1
            number = re.findall(r"^[\d\.]+", size_str)
            number = float(number[0]) if number else 0.0
            multiplier_s = re.findall(r"[KMGT][iI]?B", size_str)
            multiplier_s = multiplier_s[0] if multiplier_s else "B"
            base_multiplier = 1024 if multiplier_s.lower().endswith("ib") else 1000
            power = "KMGT".find(multiplier_s[0]) + 1 if multiplier_s[0] in "KMGT" else 0
            multiplier = base_multiplier**power

            logger.debug(
                f"Parsed size string '{size_str}' as number={number} and multiplier={multiplier}, base multiplier={base_multiplier}, power={power}"
            )
            return number * multiplier
        except Exception as e:
            logger.error(f"Error parsing data size string '{size_str}': {e}")
            return 0.0

    def get_stats(self) -> pd.DataFrame:
        with self.stats_lock:
            return self.stats_recorded.copy()

    def add_stats(self, stats: dict):
        logger.debug(f"Adding stats: {stats}")
        with self.stats_lock:
            new_df = pd.DataFrame([stats])
            self.stats_recorded = pd.concat(
                [self.stats_recorded, new_df], ignore_index=True
            )

    def record_stats(self):
        try:
            stat_fmt = ",".join(
                [f"{{{{.{v}}}}}" for v in self.stats_map.values() if v is not None]
            )

            result = subprocess.run(
                f"docker stats {self.container_name} --no-stream --format 'json'",
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )
            output = json.loads(result.stdout.strip())
            logger.debug(
                f"Raw docker stats output for container {self.container_name}: {output}"
            )
            record = {
                "timestamp": pd.Timestamp.now(),
            } | {
                k: self.stats_parsers[k](output[ok] if ok is not None else None)
                for k, ok in self.stats_map.items()
            }
            self.add_stats(record)
        except Exception as e:
            logger.error(
                f"Error recording stats for container {self.container_name}: {e}"
            )
            traceback.print_exc()

    def start_recording(self, interval_seconds=0.1):

        if hasattr(self, "_record_thread") and self._record_thread.is_alive():
            logger.warning(
                f"Stat recording already running for container {self.container_name}"
            )
            return
        self._stop_event = threading.Event()

        def record_loop():
            while not self._stop_event.is_set():
                self.record_stats()
                self._stop_event.wait(interval_seconds)

        self._record_thread = threading.Thread(target=record_loop)
        self._record_thread.start()

    def stop_recording(self):
        if hasattr(self, "_stop_event") and hasattr(self, "_record_thread"):
            self._stop_event.set()
            self._record_thread.join()

    def clear_stats(self):
        with self.stats_lock:
            self.stats_recorded = pd.DataFrame(
                columns=[
                    "timestamp",
                    "cpu_percent",
                    "mem_usage",
                    "net_io",
                    "block_io",
                    "pids",
                ]
            )


class BaseDocker(BaseDB):
    def __init__(
        self,
        id: str,
        port_id: int,
        dataset: BaseDataset,
        docker_image: str,
        endpoint: str | None = None,
        container_name: str = __name__,
        logger_dir=Path("./logs"),
        name: str = __name__,
        base_dir: Path | None = None,
        db_dir: Path | None = None,
        use_encoded_ttl: bool = False,
        do_image_build: bool = False,
        build_dir: Path = Path("./"),
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
        self.docker_image = docker_image
        self.docker_container_name = container_name
        self.base_dir = (
            base_dir if base_dir is not None else Path("./scratch") / container_name
        )
        self.db_dir = db_dir if db_dir is not None else self.base_dir / "db"
        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True, exist_ok=True)
        self.container_log_file = self.db_dir / f"{self.id}_container.log"
        self.container_log_file_fd: TextIOWrapper = self.container_log_file.open("a")
        self.build_dir = build_dir
        self.do_image_build = do_image_build
        self.stat_recorder = StatRecorder(self.docker_container_name)

    def build_image(self):
        if self.do_image_build:
            logger.info(
                f"Building docker image {self.docker_image} from {self.build_dir}"
            )
            self.run_command(f"docker build -t {self.docker_image} {self.build_dir}")

    @abstractmethod
    def setup(self):
        pass

    def stop(self):
        logger.info(f"Stopping server with container name {self.docker_container_name}")
        self.run_command(f"docker stop {self.docker_container_name}", allow_fail=True)
        self.run_command(f"docker rm -f {self.docker_container_name}", allow_fail=True)
        self.stop_record_stats()

    def start_record_stats(self):
        self.stat_recorder.clear_stats()
        self.stat_recorder.start_recording()

    def stop_record_stats(self):
        self.stat_recorder.stop_recording()

    def get_stats(self) -> pd.DataFrame:
        return self.stat_recorder.get_stats()

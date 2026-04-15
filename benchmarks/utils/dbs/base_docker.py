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

from ..datasets.base_dataset import BaseDataset
from .base_db import BaseDB
from .stats.docker import DockerStatRecorder

import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
        self.stat_recorder = DockerStatRecorder(self.docker_container_name)

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

from abc import ABC, abstractmethod
import logging
from pathlib import Path

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

import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseDocker(BaseDB):
    def __init__(
        self,
        dataset: BaseDataset,
        endpoint: str | None = None,
        container_name: str = __name__,
        logger_dir=Path("./logs"),
        *args,
        **kwargs,
    ):
        super().__init__(dataset=dataset, endpoint=endpoint, logger_dir=logger_dir)
        self.docker_container_name = container_name

    @abstractmethod
    def setup(self):
        pass

    def stop(self):
        logger.info(f"Stopping server with container name {self.docker_container_name}")
        self.run_command(f"docker stop {self.docker_container_name}", allow_fail=True)
        self.run_command(f"docker rm -f {self.docker_container_name}", allow_fail=True)

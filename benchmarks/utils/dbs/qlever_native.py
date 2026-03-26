from email.mime import base
from io import TextIOWrapper
import logging
from math import log
from os import unlink
import os
import time

import pandas
import rdflib.store
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from .executable_db import ExecutableDB
from ..datasets.base_dataset import BaseDataset, QUERY_TYPE, DataTensor
import shutil
from pathlib import Path
import rdflib
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QleverDBNative(ExecutableDB):
    port_id: int = 8030

    def __init__(
        self,
        base_dir: Path,
        dataset: BaseDataset,
        id: str = "default",
        use_encoded_ttl: bool = False,
        name: str = "QLever Native (Extended)",
        port_offset: int = 0,
    ):
        super().__init__(
            id=id,
            port_id=self.port_id + 1 + port_offset,
            dataset=dataset,
            name=name,
            use_encoded_ttl=use_encoded_ttl,
            base_dir=base_dir,
        )
        logger.info(
            f"Initialized QLeverDBNative with id={id}, port_id={self.port_id}, dataset={dataset.name}, name={name}, use_encoded_ttl={use_encoded_ttl}, endpoint={self.endpoint}"
        )

    def setup(self):
        # load the dataset into the QLever server
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Loading dataset into QLever server from {self.dataset.get_ttl_file()}"
        )
        full_ttl = (
            self.dataset.get_encoded_ttl_file()
            if self.use_encoded_ttl
            else self.dataset.get_ttl_file()
        )
        has_index = (
            self.db_dir.exists()
            and len(
                [
                    item
                    for item in self.db_dir.iterdir()
                    if self.id in item.name and item.name.endswith(".json")
                ]
            )
            > 0
        )
        if has_index:
            logger.warning(
                f"DB directory {self.db_dir} already exists, removing locks if any"
            )
            # (self.db_dir / "tdb.lock").unlink(missing_ok=True)
            # for item in self.db_dir.iterdir():
            #     if item.is_dir():
            #         (item / "tdb.lock").unlink(missing_ok=True)
        else:
            self.run_command(
                f"qlever-index -f {full_ttl.absolute()} -i {self.id}",
                cwd=self.db_dir,
            )
        try:
            self.stop()
        except subprocess.CalledProcessError:
            pass
        logger.info(f"Starting QLever server on port {self.port_id}")
        qlever_start_cmd = f"qlever-server -i {self.id} --port {self.port_id} -k 0 -m 8G --tensor-search-max-num-threads 4"
        logger.info(f"Running command: {qlever_start_cmd}")
        self.server_log_file_fd = open(self.server_log_file, "a")
        self.server = subprocess.Popen(
            qlever_start_cmd,
            cwd=self.db_dir,
            shell=True,
            stdout=self.server_log_file_fd,
            stderr=self.server_log_file_fd,
        )
        self.wait_for_server(timeout=120)

    def get_available_query_types(self) -> list[QUERY_TYPE]:
        return [QUERY_TYPE.EMBEDDED, QUERY_TYPE.INDEX, QUERY_TYPE.TWO_STAGE]

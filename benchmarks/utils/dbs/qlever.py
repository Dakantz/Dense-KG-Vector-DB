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

from .base_docker import BaseDocker
from ..datasets.base_dataset import BaseDataset, QUERY_TYPE, DataTensor
import shutil
from pathlib import Path
import rdflib
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QleverDB(BaseDocker):
    port_id: int = 5030

    def __init__(
        self,
        base_dir: Path,
        dataset: BaseDataset,
        id: str = "default",
        use_encoded_ttl: bool = False,
        name: str = "QLever (Extended)",
    ):
        port_id = self.port_id + 1
        endpoint = f"http://localhost:{port_id}/{id}/sparql"
        super().__init__(
            id=id,
            dataset=dataset,
            endpoint=endpoint,
            container_name=f"qlever_benchmarks_{id}",
            name=name,
            base_dir=base_dir,
            db_dir=base_dir / "qlever_db",
            docker_image="qlever:tensors",
            use_encoded_ttl=use_encoded_ttl,
        )
        self.port_id = port_id
        self.server: subprocess.Popen | None = None

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
        uid = os.getuid()
        gid = os.getgid() + 200  # macos ???
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
                f"docker run --rm -v {self.db_dir.absolute()}:/data -v {full_ttl.parent.absolute()}:/ttl -e UID={uid} -e GID={gid} -w /data {self.docker_image} 'qlever-index -f /ttl/{full_ttl.name} -i {self.id}'",
            )
        try:
            self.stop()
        except subprocess.CalledProcessError:
            pass
        logger.info(
            f"Starting QLever server with container name {self.docker_container_name} on port {self.port_id}"
        )
        docker_start_cmd = f"docker run --name {self.docker_container_name} -e UID={uid} -e GID={gid} -p {self.port_id}:5030 -v {self.db_dir.absolute()}:/data -w /data  {self.docker_image} 'qlever-server -i {self.id} --port 5030 -k 0'"
        logger.info(f"Running command: {docker_start_cmd}")
        self.container_log_file_fd = open(self.container_log_file, "a")
        self.server = subprocess.Popen(
            docker_start_cmd,
            shell=True,
            stdout=self.container_log_file_fd,
            stderr=self.container_log_file_fd,
        )
        self.wait_for_server(timeout=120)

    def stop(self):
        logger.info(f"Stopping server with container name {self.docker_container_name}")
        if self.server is not None:
            self.server.kill()
        self.run_command(f"docker stop {self.docker_container_name}", allow_fail=True)
        self.run_command(f"docker rm -f {self.docker_container_name}", allow_fail=True)
        if self.container_log_file_fd is not None:
            self.container_log_file_fd.close()

    def get_available_query_types(self) -> list[QUERY_TYPE]:
        return [QUERY_TYPE.EMBEDDED, QUERY_TYPE.INDEX, QUERY_TYPE.TWO_STAGE]

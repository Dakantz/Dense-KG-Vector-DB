import logging
from os import unlink
import time

import pandas
import rdflib.store
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from .base_docker import BaseDocker
from ..datasets.base_dataset import BaseDataset
import shutil
from pathlib import Path
import rdflib
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FusekiDB(BaseDocker):
    port_id: int = 3030

    def __init__(
        self,
        base_dir: Path,
        dataset: BaseDataset,
        id: str = "default",
        build_dir: Path = Path("./jena-datatensor"),
    ):
        port_id = self.port_id + 1
        endpoint = f"http://localhost:{port_id}/{id}/sparql"
        super().__init__(
            dataset=dataset, endpoint=endpoint, container_name=f"fuseki_benchmarks_{id}"
        )
        self.port_id = port_id
        self.id = id

        self.base_dir = base_dir
        self.db_dir = base_dir / "db"
        self.docker_image = "jena-datatensor"
        self.build_dir = build_dir
        self.server: subprocess.Popen | None = None

    def setup(self):
        # load the dataset into the Fuseki server using the command line tool from the ttls from the dataset
        # by generating tdb2 file and then starting the docker container with the tdb2 file as volume
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Building docker image {self.docker_image} from {self.build_dir}")
        self.run_command(f"docker build -t {self.docker_image} {self.build_dir}")
        logger.info(
            f"Loading dataset into Fuseki server from {self.dataset.get_ttl_file()}"
        )
        full_ttl = self.dataset.get_ttl_file()
        if self.db_dir.exists() and any(self.db_dir.iterdir()):
            logger.warning(
                f"DB directory {self.db_dir} already exists, removing locks if any"
            )
            (self.db_dir / "tdb.lock").unlink(missing_ok=True)
            for item in self.db_dir.iterdir():
                if item.is_dir():
                    (item / "tdb.lock").unlink(missing_ok=True)
        else:
            self.run_command(
                f"docker run --rm -v {self.db_dir.absolute()}:/tdb -v {full_ttl.parent.absolute()}:/ttl {self.docker_image} tdb2.tdbloader --loc /tdb /ttl/{full_ttl.name}"
            )
        try:
            self.stop()
        except subprocess.CalledProcessError:
            pass
        logger.info(
            f"Starting Fuseki server with container name {self.docker_container_name} on port {self.port_id}"
        )
        self.server = subprocess.Popen(
            f"docker run -d --name {self.docker_container_name} -p {self.port_id}:3030 -v {self.db_dir.absolute()}:/data {self.docker_image} custom-fuseki-server -loc=/data /{self.id}",
            shell=True,
            stdout=self.log_file_fd,
            stderr=self.log_file_fd,
        )
        time.sleep(2)  # wait for the server to start

    def stop(self):
        logger.info(f"Stopping server with container name {self.docker_container_name}")
        if self.server is not None:
            self.server.kill()
        self.run_command(f"docker stop {self.docker_container_name}", allow_fail=True)
        self.run_command(f"docker rm -f {self.docker_container_name}", allow_fail=True)

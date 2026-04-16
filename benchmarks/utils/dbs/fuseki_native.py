import logging
from os import unlink
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
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FusekiDBNative(ExecutableDB):
    port_id: int = 15030

    def __init__(
        self,
        base_dir: Path,
        dataset: BaseDataset,
        id: str = "default",
        exec_dir: Path = Path("../jena-datatensor"),
        use_encoded_ttl: bool = False,
        do_image_build: bool = False,
        name: str = "FusekiDB + RDFTensor",
        port_offset: int = 0,
    ):
        super().__init__(
            id=id,
            port_id=self.port_id + 1 + port_offset,
            base_dir=base_dir,
            dataset=dataset,
            name=name,
            use_encoded_ttl=use_encoded_ttl,
        )
        self.exec_dir = exec_dir

    def setup(self):
        # load the dataset into the Fuseki server using the command line tool from the ttls from the dataset
        # by generating tdb2 file and then starting the docker container with the tdb2 file as volume
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.kill_existing_processes()
        logger.info(
            f"Loading dataset into Fuseki server from {self.dataset.get_ttl_file()}"
        )
        full_ttl = (
            self.dataset.get_encoded_ttl_file()
            if self.use_encoded_ttl
            else self.dataset.get_ttl_file()
        )
        has_db = self.db_dir.exists() and len([*self.db_dir.rglob("*.dat")]) > 0
        if has_db:
            logger.warning(
                f"DB directory {self.db_dir} already exists, removing locks if any"
            )
            (self.db_dir / "tdb.lock").unlink(missing_ok=True)
            for item in (self.db_dir / "Data-0001").iterdir():
                if item.is_dir():
                    (item / "tdb.lock").unlink(missing_ok=True)
        else:
            print(
                f"DB directory {self.db_dir} does not exist, creating it and loading data"
            )
            self.run_command(
                f"tdb2.tdbloader --loc {self.db_dir.absolute()} {full_ttl.absolute()}",
            )
        try:
            self.stop()
        except subprocess.CalledProcessError:
            pass
        logger.info(f"Starting Fuseki server port {self.port_id}")
        if self.server is not None:
            logger.warning("Server is already running, stopping it first")
            self.stop()

        self.server_log_file_fd = self.server_log_file.open("a")
        tensor_cp = os.environ.get("TENSOR_CP")
        if tensor_cp is None:
            logger.error("TENSOR_CP environment variable is not set")
            tensor_cp = Path(self.exec_dir) / "jena-datatensor" / "target/*"
            tensor_cp = tensor_cp.resolve()
            logger.info(f"Using default TENSOR_CP={tensor_cp}")

        fuseki_home = os.environ.get("FUSEKI_HOME")
        if fuseki_home is None:
            logger.error("FUSEKI_HOME environment variable is not set")
            fuseki_home = os.getenv("HOME")
            fuseki_home = Path(fuseki_home) / "apache-jena-fuseki-5.2.0"
            fuseki_home = fuseki_home.resolve()
            logger.info(f"Using default FUSEKI_HOME={fuseki_home}")
        logger.info(f"Using FUSEKI_HOME={fuseki_home} and TENSOR_CP={tensor_cp}")
        self.server = subprocess.Popen(
            f"FUSEKI_HOME={fuseki_home} TENSOR_CP={tensor_cp} ./custom-fuseki-server -loc={self.db_dir.absolute()} --port {self.port_id} /{self.id}",
            shell=True,
            cwd=self.exec_dir,
            stdout=self.server_log_file_fd,
            stderr=self.server_log_file_fd,
        )
        self.pid = self.server.pid
        self.wait_for_server()

    def get_available_query_types(self) -> list[QUERY_TYPE]:
        return [QUERY_TYPE.EMBEDDED, QUERY_TYPE.TWO_STAGE]

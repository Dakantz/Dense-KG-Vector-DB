from calendar import c
import logging
from os import unlink
import re
from socket import timeout
import time

from neo4j import GraphDatabase
import pandas
from rdflib.query import Result
import rdflib.store
from rdflib.plugins.stores.sparqlstore import SPARQLStore
import tqdm


from .executable_db import ExecutableDB
from ..datasets.base_dataset import (
    BaseDataset,
    QUERY_TYPE,
    DataTensor,
    QUERY_DIFFICULTY,
)
import shutil
from pathlib import Path
import rdflib
import subprocess
import os
from .utils import run_with_timeout
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Neo4JDB(ExecutableDB):
    def __init__(
        self,
        base_dir: Path,
        dataset: BaseDataset,
        id: str = "default",
        exec_dir: Path = Path("../neo4j-community-2026.04.0"),
        use_encoded_ttl: bool = False,
        name: str = "Neo4J",
        force_recreate: bool = False,
    ):
        super().__init__(
            id=id,
            base_dir=base_dir,
            dataset=dataset,
            name=name,
            use_encoded_ttl=use_encoded_ttl,
            port_id=7687,
            port=7687,
        )
        self.exec_dir = exec_dir
        self.default_config_dir = self.exec_dir / "conf"
        self.g = None
        self.force_recreate = force_recreate

        self.URI = "neo4j://localhost:7687"
        self.AUTH = ("neo4j", "testingpass")
        # self.dbms_dir = dbms_dir
        # self.db_dir = self.dbms_dir / self.dataset.data_dir.name

    def driver(self):
        return GraphDatabase.driver(self.URI, auth=self.AUTH)

    def get_any_result(self):
        with self.driver() as driver:
            result = driver.verify_connectivity(connection_acquisition_timeout=1)
            return result

    def reencode(self):
        with self.driver() as driver:
            result = driver.execute_query("MATCH (n) RETURN DISTINCT keys(n) AS keys;")
            all_keys: set[str] = set()
            for record in result.records:
                all_keys.update(record.data()["keys"])
            embedding_keys = [key for key in all_keys if key.endswith("_embedding")]
            print(f"Found {len(embedding_keys)} embedding keys: {embedding_keys}")
            g = tqdm.tqdm(desc="Processing records: ", unit=" records")
            for key in embedding_keys:
                vector_key = key.replace("_embedding", "_embedding_vector")
                for record in driver.execute_query(
                    f"MATCH (n) WHERE n.{key} IS NOT NULL RETURN n.{key} AS value, elementId(n) AS id"
                ).records:
                    value = record["value"]
                    if not isinstance(value, str):
                        print(f"Warning: value of {key} is not a string: {value}")
                        continue
                    try:
                        vector = DataTensor.from_literal(value).data
                        driver.execute_query(
                            f"MATCH (n) WHERE elementId(n) = '{record['id']}' SET n.{vector_key} = {vector};"
                        )
                        g.update(1)
                    except Exception as e:
                        print(f"Error parsing value of {key}: {e}")
                        continue
            for key in embedding_keys:
                logger.info(f"Creating index for {key}")
                vector_key = key.replace("_embedding", "_embedding_vector")
                driver.execute_query(f"""DROP INDEX {vector_key}_index IF EXISTS;""")
                driver.execute_query(f"""CREATE VECTOR INDEX {vector_key}_index IF NOT EXISTS
                                    FOR (n:Resource) ON n.{vector_key} OPTIONS {{
                                    indexConfig: {{
                                        }}
                                    }};""")

    def setup(self):
        # load the dataset into the Neo4J server using the command line tool from the ttls from the dataset
        # by generating a database directory and then starting the Neo4J server with the database directory
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir = self.db_dir / "conf"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        #
        conf_file = self.config_dir / "neo4j.conf"
        edited_conf = ""
        dbname = self.db_dir.name + self.dataset.data_dir.name
        dbname = re.sub(r"_", "", dbname)
        with open(self.default_config_dir / "neo4j.conf", "r") as f:
            default_conf = f.read()
            for line in default_conf.splitlines():
                if line.startswith("initial.dbms.default_database"):
                    edited_conf += f"initial.dbms.default_database={dbname}\n"
                else:
                    edited_conf += line + "\n"
        with open(conf_file, "w") as f:
            f.write(edited_conf)
        logger.info(f"Copied default neo4j.conf to {conf_file}")
        logger.info(f"Setting initial database to {dbname} in {conf_file}")

        self.kill_existing_processes()

        logger.info(f"Starting Neo4J server with config from {conf_file}")
        if self.server is not None:
            logger.warning("Server is already running, stopping it first")
            self.stop()
        pid_file = self.exec_dir / "run" / "neo4j.pid"
        if pid_file.exists():
            logger.warning(f"PID file {pid_file} already exists, deleting it")
            pid_file.unlink()
        self.server_log_file_fd = self.server_log_file.open("a")

        self.server = subprocess.Popen(
            f"NEO4J_CONF={self.config_dir.absolute()} ./bin/neo4j-admin server console",
            shell=True,
            cwd=self.exec_dir,
            stdout=self.server_log_file_fd,
            stderr=self.server_log_file_fd,
        )
        self.wait_for_server(timeout=60)
        try:
            with self.driver() as driver:
                driver.execute_query("CALL n10s.graphconfig.init();")
                driver.execute_query(
                    "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS  FOR (r:Resource) REQUIRE r.uri IS UNIQUE;"
                )
                for prefix, ns in self.dataset.prefixes.items():
                    driver.execute_query(
                        f"CALL n10s.nsprefixes.add('{prefix}', '{ns}');"
                    )
        except Exception as e:
            logger.error(f"Error initializing Neo4J graph config: {e}")
        full_ttl = (
            self.dataset.get_encoded_ttl_file()
            if self.use_encoded_ttl
            else self.dataset.get_ttl_file()
        )
        count_tuples = self.get_triple_count()
        if count_tuples > 128 and not self.force_recreate:
            logger.warning(
                f"DB directory {self.db_dir} already exists, skipping data loading (found {count_tuples} tuples in the database)"
            )
        else:
            logger.info(
                f"DB {self.db_dir} does not exist (fully), creating it and loading data (got {count_tuples} tuples in the database)"
            )
            with self.driver() as driver:
                logger.info(
                    f"Loading dataset into Neo4J server from {self.dataset.get_ttl_file()}"
                )
                # https://github.com/neo4j-labs/neosemantics/issues/236
                fetch_result = driver.execute_query(
                    f'CALL n10s.rdf.import.fetch("file://{full_ttl.absolute()}", "Turtle", {{verifyUriSyntax: false}});'
                )
                logger.info(
                    f"Finished loading dataset into Neo4J server, result: {fetch_result}')"
                )
            self.reencode()
        for retry in range(5):
            try:
                with pid_file.open("r") as f:
                    self.pid = int(f.read().strip())
                break
            except Exception as e:
                logger.warning(
                    f"Neo4J server not ready yet, retrying... ({retry + 1}/5)"
                )
                time.sleep(5)

    def raw_query(self, sparql_query: str) -> Result:
        logger.debug(
            f"Running SPARQL query: {sparql_query} against endpoint {self.endpoint}"
        )

        def query_operation():
            with self.driver() as driver:
                qres = driver.execute_query(sparql_query)
                return qres

        return run_with_timeout(query_operation, timeout=self.timeout)

    def q_to_df_values(self, qres: any, remove_ns: bool = True):
        first_record = (
            qres.records[0] if qres.records and len(qres.records) > 0 else None
        )
        if not qres.keys and not first_record:
            return pd.DataFrame()
        results = [rec.data() for rec in qres.records]  # type: ignore
        results_df = pd.DataFrame(results)
        # results_df = results_df.map(self.to_readable)
        return results_df

    def query_auto(
        self,
        tensor: DataTensor | None = None,
        query_difficulty: QUERY_DIFFICULTY = None,
        query_type: QUERY_TYPE = QUERY_TYPE.EMBEDDED,
    ) -> pd.DataFrame:
        available_queries = self.get_queries(embedding=tensor)
        if query_difficulty not in available_queries:
            raise ValueError(
                f"Query difficulty {query_difficulty} not supported by this database. Supported query difficulties: {list(available_queries.keys())}"
            )
        query_types = available_queries[query_difficulty]
        if query_type not in query_types:
            raise ValueError(
                f"Query type {query_type} not supported by this database. Supported query types: {query_types.keys()}"
            )

        def query_operation():
            with self.driver() as driver:
                qres = driver.execute_query(query_types[query_type])
                return self.q_to_df_values(qres)

        return run_with_timeout(query_operation, timeout=self.timeout)

    def query(
        self,
        sparql_query: str,
        remove_ns: bool = True,
    ) -> pd.DataFrame:
        logger.debug(
            f"Running SPARQL query: {sparql_query} against endpoint {self.endpoint}"
        )

        def query_operation():
            logger.debug(
                f"Running SPARQL query: {sparql_query} against endpoint {self.endpoint}"
            )
            with self.driver() as driver:
                qres = driver.execute_query(sparql_query)
                return self.q_to_df_values(qres, remove_ns=remove_ns)

        return run_with_timeout(query_operation, timeout=self.timeout)

    def get_available_query_types(self) -> list[QUERY_TYPE]:
        return [QUERY_TYPE.CYPHER_EMBEDDED, QUERY_TYPE.CYPHER_INDEX]

    def get_triple_count(self):
        with self.driver() as driver:
            result = driver.execute_query("MATCH (n) RETURN count(n) AS count;")
            count_tuples = result.records[0]["count"] if result.records else 0
            return count_tuples

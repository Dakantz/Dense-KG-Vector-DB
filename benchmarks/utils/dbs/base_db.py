from abc import ABC, abstractmethod
from io import TextIOWrapper
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

import time
from .utils import run_with_timeout
from .stats.base import BaseStatRecorder
from ..datasets.base_dataset import (
    BaseDataset,
    QUERY_TYPE,
    QUERY_DIFFICULTY,
    DataTensor,
    mixin_queries,
)
from .dense_index_cache import DenseIndexCache

import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


INT_COMPATIBLE_TYPES = [
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
]
FLOAT_COMPATIBLE_TYPES = [
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#double",
    "http://www.w3.org/2001/XMLSchema#decimal",
    # kilogram, seconds
]
two_stage_mixin: dict[QUERY_DIFFICULTY] = {}
for difficulty in QUERY_DIFFICULTY:
    class_name = f"{difficulty.value.capitalize()}TwoStageQueryMixin"
    t = type(
        class_name,
        (object,),
        {
            f"query_{difficulty.value}_two_stage": abstractmethod(
                lambda self, embedding: NotImplementedError(
                    f"query_{difficulty.value}_two_stage not implemented for {self.__class__.__name__}"
                )
            )
        },
    )
    two_stage_mixin[difficulty] = t

PORT_COUNTER = 14012


class BaseDB(
    two_stage_mixin[QUERY_DIFFICULTY.EASY], two_stage_mixin[QUERY_DIFFICULTY.HARD], ABC
):
    def __init__(
        self,
        dataset: BaseDataset,
        id: str,
        port_id: int = 3333,
        endpoint: str | None = None,
        logger_dir=Path("./logs"),
        prefixes: dict[str, str] | None = None,
        name: str = __name__,
        use_encoded_ttl: bool = False,
        timeout: int = 30,
        *args,
        **kwargs,
    ):
        global PORT_COUNTER
        logger_dir.mkdir(exist_ok=True)
        self.port_id = port_id + PORT_COUNTER
        PORT_COUNTER += 1
        self.endpoint = f"http://localhost:{self.port_id}/{id}/sparql"
        self.id = id
        self.timeout = timeout
        self.name = name

        self.store = SPARQLStore(
            self.endpoint,
            method="POST",
        )
        self.dataset = dataset
        self.log_file = logger_dir / f"{self.__class__.__name__}.log"
        self.log_file_fd = open(self.log_file, "w")
        self.prefixes: dict[str, str] = {} if prefixes is None else prefixes
        self.g = rdflib.Graph(store=self.store)
        for prefix, uri in self.prefixes.items() | self.dataset.prefixes.items():
            logger.debug(f"Binding prefix {prefix} to URI {uri}")
            self.g.bind(prefix, uri)
        self.dense_cache: DenseIndexCache = DenseIndexCache()
        self.use_encoded_ttl = use_encoded_ttl
        self.stat_recorder: BaseStatRecorder | None = None

    def kill_existing_processes(self):
        pass

    def run_command(
        self,
        command: str,
        allow_fail=False,
        cwd: Path | None = None,
        log_file_fd: TextIOWrapper | None = None,
    ):
        logger.debug(f"Running command: {command}")
        self.log_file_fd.write(f"Running command: {command}\n")
        log_file = log_file_fd or self.log_file_fd
        log_file.write(f"Running command: {command}\n")
        try:
            process = subprocess.run(
                command,
                shell=True,
                check=True,
                stdout=log_file_fd,
                stderr=log_file_fd,
                errors="replace",
                cwd=cwd,
            )
            return process
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            # logger.error(f"Command output: {e.output}")
            if not allow_fail:
                raise e
            else:
                return None

    @abstractmethod
    def setup(self):
        pass

    def stop(self):
        pass

    def wait_for_server(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = self.g.query("ASK { ?s ?p ?o }")
                logger.info("Server is up and responding to queries")
                if not isinstance(result, tuple):
                    return True
                else:
                    logger.info(
                        f"Server is not responding to queries yet, got result: {result}"
                    )
                    raise ValueError("Server is not responding to queries yet")
            except Exception as e:
                logger.info(
                    f"Waiting for server to start..., got error: {e} ({self.endpoint})"
                )
                time.sleep(1)
        raise TimeoutError(f"Server did not start within {timeout} seconds")

    def __enter__(self):
        self.setup()
        logger.debug("Database setup complete, entering context manager")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug("Exiting context manager, stopping database")
        self.stop()

    def raw_query(self, sparql_query: str) -> Result:
        logger.debug(
            f"Running SPARQL query: {sparql_query} against endpoint {self.endpoint}"
        )

        def query_operation():
            qres = self.g.store.query(sparql_query)
            return qres

        return run_with_timeout(query_operation, timeout=self.timeout)

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
        if query_type == QUERY_TYPE.TWO_STAGE:
            return self.two_stage_query(
                query_types[query_type],
                tensor=tensor,
                query_difficulty=query_difficulty,
            )

        def query_operation():
            qres = self.g.query(query_types[query_type])
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
            qres = self.g.query(sparql_query)
            return self.q_to_df_values(qres, remove_ns=remove_ns)

        return run_with_timeout(query_operation, timeout=self.timeout)

    # two-stage query generation and execution
    def query_easy_two_stage(
        self, qres_df: pd.DataFrame, embedding: DataTensor, k: int
    ) -> pd.DataFrame:
        if embedding is None:
            raise ValueError("Embedding must be provided for two-stage query")
        query_key = f"{QUERY_DIFFICULTY.EASY.value}_{QUERY_TYPE.TWO_STAGE.value}"
        distances, indices = self.dense_cache.find_knn(
            query_key, embedding, k=k, vectors=qres_df["vector"]
        )
        qres_df_filtered = qres_df.iloc[indices].copy()
        qres_df_filtered["dist"] = distances
        return qres_df_filtered

    def query_hard_two_stage(
        self, qres_df: pd.DataFrame, embedding: DataTensor, k: int
    ) -> pd.DataFrame:
        query_key = f"{QUERY_DIFFICULTY.HARD.value}_{QUERY_TYPE.TWO_STAGE.value}"
        qres_df["dist"] = 0.0
        closest_df = pd.DataFrame(columns=qres_df.columns.tolist() + ["dist"])
        for i, row in qres_df.iterrows():
            vectorA = DataTensor.from_literal(row["vectorA"])
            distances, indices = self.dense_cache.find_knn(
                f"{query_key}_B", vectorA, k=k, vectors=qres_df["vectorB"]
            )
            closest_rows = qres_df.iloc[indices].copy()
            closest_rows["dist"] = distances
            closest_df = pd.concat([closest_df, closest_rows], ignore_index=True)
        qres_df_filtered = closest_df.sort_values("dist").head(k)
        return qres_df_filtered

    def two_stage_query(
        self,
        sparql_query: str,
        tensor: DataTensor | None = None,
        query_difficulty: QUERY_DIFFICULTY = QUERY_DIFFICULTY.EASY,
        k: int = 10,
    ) -> pd.DataFrame:
        logger.debug(
            f"Running two-stage SPARQL query: {sparql_query} against endpoint {self.endpoint}"
        )
        qres = self.g.query(sparql_query)
        qres_df = self.q_to_df_values(qres)
        if query_difficulty == QUERY_DIFFICULTY.EASY:
            qres_df_filtered = self.query_easy_two_stage(qres_df, embedding=tensor, k=k)
        elif query_difficulty == QUERY_DIFFICULTY.HARD:
            qres_df_filtered = self.query_hard_two_stage(qres_df, embedding=tensor, k=k)
        return qres_df_filtered

    def to_readable_literals(self, cls: str | Literal | URIRef):
        if isinstance(cls, Literal):
            return cls.value
        else:
            return cls

    def to_readable(self, cls: str | Literal | URIRef):
        if isinstance(cls, Literal):
            value = cls.title()
            if cls.datatype is not None:
                try:
                    cls_dtype = str(cls.datatype)
                    if cls_dtype in INT_COMPATIBLE_TYPES:
                        value = int(value)
                    elif (
                        cls_dtype in FLOAT_COMPATIBLE_TYPES
                        or "kilogram" in cls.datatype
                        or "metre" in cls.datatype
                        or "seconds" in cls.datatype
                        or "minute" in cls.datatype
                        or "hour" in cls.datatype
                        or "day" in cls.datatype
                    ):
                        value = float(value)
                except Exception as e:
                    print(traceback.format_exc())
                    print("Failed to convert", value, "to int or float", e)
            return value

        elif isinstance(cls, URIRef) or hasattr(cls, "n3"):
            return cls.n3(self.g.namespace_manager)  # type: ignore
        else:
            return cls

    def __q_to_df(self, q: str):
        results = list(self.g.query(q))
        # for r in results:
        #     for t in r:
        #         if isinstance(t, URIRef):
        #             print(t.n3(self.g.namespace_manager))
        #         else:
        #             print(t)
        # return pd.DataFrame(results).map(
        #     lambda x: x.n3(self.g.namespace_manager) if hasattr(x, "n3") else x
        # )
        return pd.DataFrame(results)

    def q_to_df_values(self, qres: Result, remove_ns: bool = True) -> pd.DataFrame:
        if isinstance(qres, tuple):
            logger.error(f"Query failed: {qres}")
        if not qres.vars:
            return pd.DataFrame()
        cols = [str(var) for var in qres.vars]
        results = [dict(zip(cols, row)) for row in qres]  # type: ignore
        results_df = pd.DataFrame(results)
        results_df = results_df.map(self.to_readable)
        if remove_ns:
            results_df = self.remove_ns_from_df(results_df)
        return results_df

    def remove_prefix(self, uri: str) -> str:
        for prefix, namespace in self.prefixes.items() | self.dataset.prefixes.items():
            if uri.startswith(f"<{namespace}") or uri.startswith(f"{namespace}"):
                offset = 1 if uri.startswith("<") else 0
                return f"{prefix}:{uri[len(str(namespace)) + offset : -1]}"
        return uri

    def remove_ns_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df_c = df.copy()
        for column in df.columns:
            df_c[column] = df.apply(
                lambda row: self.remove_prefix(str(row[column])), axis=1
            )
        return df_c

    def get_available_query_types(self) -> list[QUERY_TYPE]:
        return [QUERY_TYPE.EMBEDDED, QUERY_TYPE.INDEX, QUERY_TYPE.TWO_STAGE]

    def get_queries(
        self,
        embedding: DataTensor,
    ) -> dict[QUERY_DIFFICULTY, dict[QUERY_TYPE, str]]:
        available_query_types = self.get_available_query_types()
        queries: dict[QUERY_DIFFICULTY, dict[QUERY_TYPE, str]] = {}
        for difficulty in QUERY_DIFFICULTY:
            for query_type in available_query_types:
                if isinstance(self.dataset, mixin_queries[difficulty][query_type]):
                    if (
                        not isinstance(self, two_stage_mixin[difficulty])
                        and query_type == QUERY_TYPE.TWO_STAGE
                    ):
                        raise ValueError(
                            f"Dataset {self.dataset.name} supports two-stage queries, but database {self.__class__.__name__} does not implement {two_stage_mixin[difficulty].__name__}"
                        )
                    method_name = f"get_query_{difficulty.value}_{query_type.value}"
                    method = getattr(self.dataset, method_name)
                    query = method(embedding)
                    if difficulty not in queries:
                        queries[difficulty] = {}
                    queries[difficulty][query_type] = query
        return queries

    def start_record_stats(self):
        if self.stat_recorder is not None:
            self.stat_recorder.clear_stats()
            self.stat_recorder.start_recording()
        else:
            raise NotImplementedError(
                "Stat recording not implemented for base database"
            )

    def stop_record_stats(self):
        if self.stat_recorder is not None:
            self.stat_recorder.stop_recording()
        else:
            raise NotImplementedError(
                "Stat recording not implemented for base database"
            )

    def get_stats(self) -> pd.DataFrame:
        if self.stat_recorder is not None:
            return self.stat_recorder.get_stats()
        else:
            raise NotImplementedError(
                "Stat recording not implemented for base database"
            )

    def get_triple_count(self) -> int:
        count_query = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
        result = self.query(count_query)
        if "count" in result.columns:
            return int(result["count"].iloc[0])
        else:
            raise ValueError("Count query did not return a 'count' column")

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


class BaseDB(ABC):
    def __init__(
        self,
        dataset: BaseDataset,
        endpoint: str | None = None,
        logger_dir=Path("./logs"),
        *args,
        **kwargs,
    ):
        logger_dir.mkdir(exist_ok=True)

        self.endpoint = (
            endpoint if endpoint is not None else "http://localhost:3030/default/sparql"
        )

        self.store = SPARQLStore(
            self.endpoint,
            method="POST",
            timeout=300,
        )
        self.dataset = dataset
        self.g = rdflib.Graph(store=self.store)
        self.log_file = logger_dir / f"{self.__class__.__name__}.log"
        self.log_file_fd = open(self.log_file, "w")

    def run_command(self, command: str, allow_fail=False):
        # logger.info(f"Running command: {command}")
        self.log_file_fd.write(f"Running command: {command}\n")
        try:
            process = subprocess.run(
                command,
                shell=True,
                check=True,
                stdout=self.log_file_fd,
                stderr=self.log_file_fd,
                errors="replace",
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

    def __enter__(self):
        self.setup()
        logger.info("Database setup complete, entering context manager")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("Exiting context manager, stopping database")
        self.stop()

    def query(self, sparql_query: str) -> pd.DataFrame:
        logger.info(
            f"Running SPARQL query: {sparql_query} against endpoint {self.endpoint}"
        )
        qres = self.g.query(sparql_query)

        return self.__q_to_df_values(qres)

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

    def __q_to_df_values(self, qres: Result) -> pd.DataFrame:
        if not qres.vars:
            return pd.DataFrame()
        cols = [str(var) for var in qres.vars]
        results = [dict(zip(cols, row)) for row in qres]  # type: ignore
        results_df = pd.DataFrame(results)
        results_df = results_df.map(self.to_readable)
        return results_df

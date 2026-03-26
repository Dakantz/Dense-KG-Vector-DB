from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Generator, Literal, Optional, Tuple, Any
import os
import oxrdflib
import rdflib
from sentence_transformers import SentenceTransformer
import numpy as np
from rdflib import Literal as RDFLiteral, Node
from tqdm import tqdm
from zmq import Enum
import pandas as pd
from .data_tensor import DataTensor
from .utils import parse_nt_to_generator, save_from_generator

logger = logging.getLogger(__name__)


class QUERY_DIFFICULTY(Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class QUERY_TYPE(Enum):
    EMBEDDED = "embedded"
    INDEX = "index"
    TWO_STAGE = "two_stage"


# generate all combinations of query difficulty and query type for the dataset, and implement a method to get the query for a given combination
mixin_queries: dict[QUERY_DIFFICULTY, dict[QUERY_TYPE, type]] = {}
for difficulty in [
    QUERY_DIFFICULTY.EASY,
    QUERY_DIFFICULTY.MEDIUM,
    QUERY_DIFFICULTY.HARD,
]:
    mixin_queries[difficulty] = {}
    for query_type in [QUERY_TYPE.EMBEDDED, QUERY_TYPE.INDEX, QUERY_TYPE.TWO_STAGE]:
        class_name = (
            f"{difficulty.value.capitalize()}{query_type.value.capitalize()}QueryMixin"
        )
        t = type(
            class_name,
            (object,),
            {
                f"get_query_{difficulty.value}_{query_type.value}": abstractmethod(
                    lambda self, embedding: NotImplementedError(
                        f"get_query_{difficulty.value}_{query_type.value} not implemented for {self.__class__.__name__}"
                    )
                )
            },
        )
        mixin_queries[difficulty][query_type] = t


class BaseDataset(ABC):
    """Abstract base class for datasets."""

    def __init__(
        self,
        name: str,
        data_dir: Optional[str] = None,
        prefixes: dict[str, str] | None = None,
    ):
        """
        Initialize the dataset.

        Args:
            name: Name of the dataset
            data_dir: Directory to store/load dataset files
        """
        self.name = name
        self.data_dir = Path(data_dir) if data_dir is not None else Path(os.getcwd())
        self._data = None
        self.prefixes = prefixes if prefixes is not None else {}

    def setup(self):
        pass

    def get_ttl_files(self) -> list[Path]:
        return list(self.base_dir.rglob("*.ttl"))

    def get_encoded_ttl_files(self) -> list[Path]:
        return list(self.base_dir.rglob("*_encoded.ttl"))

    def get_ttl_file(self) -> Path:
        return self.base_dir / "_complet.ttl"

    def get_encoded_ttl_file(self) -> Path:
        return self.base_dir / "_complet_encoded.ttl"

    def encode_streaming(
        self, model: SentenceTransformer, batch_size=64, force_reencode=False
    ):
        # load triples from ttl file, encode the #label using the provided model, and save the embeddings to a new .ttl file with the same structure but with an additional triple for the embedding
        if self.get_encoded_ttl_file().exists() and not force_reencode:
            print(
                f"Encoded TTL file already exists at {self.get_encoded_ttl_file()}, skipping encoding"
            )
            return

        def generate_triples(
            source: Path, batch_size: int
        ) -> Generator[tuple[Node, Node, Node]]:
            ttl_source = parse_nt_to_generator(source)
            batch_collector = []
            for triple in tqdm(ttl_source, desc="Encoding triples"):
                s, p, o = triple
                yield triple
                if str(p).endswith("#label") or str(p).endswith("#comment"):
                    batch_collector.append((s, p, o))
                if len(batch_collector) >= batch_size:
                    embeddings = model.encode(
                        [str(o) for _, _, o in batch_collector],
                        convert_to_numpy=True,
                        show_progress_bar=False,
                    )
                    for (s, p, o), embedding in zip(batch_collector, embeddings):
                        embedding_dt = DataTensor.from_numpy(embedding)
                        embedding_literal = embedding_dt.to_literal()
                        yield (
                            s,
                            rdflib.URIRef(str(p) + "_embedding"),
                            embedding_literal,
                        )
                    batch_collector = []

        counter = save_from_generator(
            self.get_encoded_ttl_file(),
            generate_triples(self.get_ttl_file(), batch_size),
        )
        return counter

    def encode(self, model: SentenceTransformer, batch_size=32, force_reencode=False):
        # load triples from ttl file, encode the #label using the provided model, and save the embeddings to a new .ttl file with the same structure but with an additional triple for the embedding
        if self.get_encoded_ttl_file().exists() and not force_reencode:
            print(
                f"Encoded TTL file already exists at {self.get_encoded_ttl_file()}, skipping encoding"
            )
            return
        g = rdflib.Graph(store="Oxigraph")
        g.parse(self.full_ttl_file, format="ox-ttl")
        g_encoding = rdflib.Graph()
        # extract all triples with a #label
        batch = []

        def encode_batch(batch):
            for s, p, o in batch:
                embedding = model.encode(
                    str(o), convert_to_numpy=True, show_progress_bar=False
                )
                # put into a json, then into a literal
                embedding_dt = DataTensor.from_numpy(embedding)
                embedding_literal = embedding_dt.to_literal()
                g_encoding.add(
                    (s, rdflib.URIRef(str(p) + "_embedding"), embedding_literal)
                )

        for s, p, o in tqdm(g, desc="Encoding triples"):
            g_encoding.add((s, p, o))

            if str(p).endswith("#label") or str(p).endswith("#comment"):
                batch.append((s, p, o))
                if len(batch) >= batch_size:
                    encode_batch(batch)
                    batch = []
        # save the new graph to a ttl file
        g_encoding.serialize(self.get_encoded_ttl_file(), format="ox-nt")

    def get_triple_count(self, encoded=False) -> int:
        # count the number of triples in the ttl file
        ttl_file = self.get_encoded_ttl_file() if encoded else self.get_ttl_file()
        with open(ttl_file, "r") as f:
            return sum(1 for _ in f)

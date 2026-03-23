from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from typing import Literal, Optional, Tuple, Any
import os
import oxrdflib
import rdflib
from sentence_transformers import SentenceTransformer
import numpy as np
from rdflib import Literal as RDFLiteral
from tqdm import tqdm


class QUERY_DIFFICULTY:
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class QUERY_TYPE:
    EMBEDDED = "embedded"
    INDEX = "index"
    TWO_STAGE = "two_stage"


@dataclass
class DataTensor:
    data: list[float]
    type: Literal["float32", "float64", "int32", "int64"]
    shape: tuple[int, ...]

    @staticmethod
    def from_numpy(array: np.ndarray) -> "DataTensor":
        return DataTensor(
            data=array.flatten().tolist(),
            type=str(array.dtype),
            shape=array.shape,
        )

    def to_numpy(self) -> np.ndarray:
        return np.array(self.data, dtype=self.type).reshape(self.shape)

    @staticmethod
    def from_literal(lit: RDFLiteral) -> "DataTensor":
        json_str = str(lit)
        data = json.loads(json_str)
        return DataTensor(
            data=data["data"],
            type=data["type"],
            shape=tuple(data["shape"]),
        )

    def to_literal(self) -> RDFLiteral:
        json_str = json.dumps(
            {
                "data": self.data,
                "type": self.type,
                "shape": self.shape,
            }
        )
        return RDFLiteral(
            json_str, datatype="https://w3id.org/rdf-tensor/datatypes#NumericDataTensor"
        )


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
        self.data_dir = data_dir or os.getcwd()
        self._data = None
        self.prefixes = prefixes if prefixes is not None else {}

    def setup(self):
        pass

    def get_ttl_files(self):
        return list(self.base_dir.rglob("*.ttl"))

    def get_encoded_ttl_files(self):
        return list(self.base_dir.rglob("*_encoded.ttl"))

    def get_ttl_file(self):
        return self.base_dir / "_complet.ttl"

    def get_encoded_ttl_file(self):
        return self.base_dir / "_complet_encoded.ttl"

    def get_available_difficulties(self) -> list[QUERY_DIFFICULTY]:
        return [QUERY_DIFFICULTY.EASY, QUERY_DIFFICULTY.MEDIUM, QUERY_DIFFICULTY.HARD]

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

    @abstractmethod
    def get_queries(
        self,
        difficulty: QUERY_DIFFICULTY,
        query_type: QUERY_TYPE,
        embedding: DataTensor,
    ) -> str:
        pass

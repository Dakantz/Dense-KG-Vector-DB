from pathlib import Path

from sentence_transformers import SentenceTransformer

from .base_dataset import BaseDataset
import subprocess
import shutil


class DBPedia(BaseDataset):
    def __init__(self, base_dir: Path):
        super().__init__(
            data_dir=base_dir,
            name="DBPedia",
            prefixes={
                "dbo": "http://dbpedia.org/ontology/",
                "dbr": "http://dbpedia.org/resource/",
            },
        )
        self.base_dir = base_dir
        self.full_ttl_file = self.base_dir / "dbpedia_complete.nt.gz"

    def setup(self):
        pass

    def get_ttl_files(self):
        return list(self.base_dir.rglob("*.nt.gz"))

    def get_ttl_file(self):
        return self.full_ttl_file

    def encode(self, model: SentenceTransformer, batch_size=32, force_reencode=False):
        # load triples from ttl file, encode the #label using the provided model, and save the embeddings to a new .ttl file with the same structure but with an additional triple for the embedding
        if self.get_encoded_ttl_file().exists() and not force_reencode:
            print(
                f"Encoded TTL file already exists at {self.get_encoded_ttl_file()}, skipping encoding"
            )
            return

    def get_triple_count(self, encoded=False) -> int:
        # count the number of triples in the ttl file
        ttl_file = self.get_encoded_ttl_file() if encoded else self.get_ttl_file()
        with open(ttl_file, "r") as f:
            return sum(1 for _ in f)

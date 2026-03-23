from pathlib import Path
import logging

from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from .base_dataset import BaseDataset, DataTensor, QUERY_DIFFICULTY, QUERY_TYPE
import subprocess
import shutil
import rdflib


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BerlinSparqlBenchmark(BaseDataset):
    def __init__(
        self,
        base_dir: Path = Path("./bsbm"),
        ttl_file: Path = "dataset.nt",
        n=int(1e9),
    ):
        super().__init__(
            name="bsbm",
            data_dir=str(base_dir),
            prefixes={
                "bsbmi": "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/",
                "bsbmv": "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/",
            },
        )
        self.base_dir = base_dir
        self.full_ttl_file = self.base_dir / ttl_file
        self.n = n

    def setup(self):
        if self.full_ttl_file.exists():
            logger.info(
                f"BSBM dataset already exists in {self.base_dir}, skipping generation"
            )
            return

        logger.info(f"Generating BSBM dataset in {self.base_dir}")
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            f"docker run -v {self.base_dir.absolute()}:/app/data -e 'DATA_DESTINATION=./' vcity/bsbm generate -pc {self.n}",
            shell=True,
            check=True,
        )

    def get_ttl_files(self):
        return [self.full_ttl_file]

    def get_encoded_ttl_files(self):
        return [self.base_dir / f"{self.full_ttl_file.stem}_encoded.nt"]

    def get_ttl_file(self):
        return self.full_ttl_file

    def get_encoded_ttl_file(self):
        return self.base_dir / f"{self.full_ttl_file.stem}_encoded.nt"

    def get_available_difficulties(self):
        return [QUERY_DIFFICULTY.EASY, QUERY_DIFFICULTY.HARD]

    def get_queries(
        self,
        difficulty: QUERY_DIFFICULTY,
        query_type: QUERY_TYPE,
        embedding: DataTensor,
    ) -> str:
        # return a list of queries for the dataset based on the difficulty and query type

        # return a list of queries for the dataset based on the difficulty and query type
        if difficulty == QUERY_DIFFICULTY.EASY:
            if query_type == QUERY_TYPE.EMBEDDED:
                return f"""
    PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
    PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
    PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
    SELECT ?product  ?vector ?dist
    WHERE {{
        ?product rdf:label_embedding ?vector .
        BIND(dtf:cosineSimilarity(?vector, {embedding.n3()}) AS ?dist) .
    }}
    ORDER BY DESC(?dist)
    LIMIT 10
    """
        elif difficulty == QUERY_DIFFICULTY.MEDIUM:
            raise NotImplementedError(
                "Medium difficulty queries are not implemented yet"
            )
        elif difficulty == QUERY_DIFFICULTY.HARD:
            if query_type == QUERY_TYPE.EMBEDDED:
                # For a specific product find 10 other similar products via their most similar features.
                return """
                    PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
                    PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
                    PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
                    SELECT ?productA ?productB ?dist ?vectorA ?vectorB
                    WHERE {
                        ?productA bsbmv:productFeature ?featureA .
                        ?featureA rdf:comment_embedding ?vectorA .
                        ?productB bsbmv:productFeature ?featureB .
                        ?featureB rdf:comment_embedding ?vectorB .
                        BIND(dtf:cosineSimilarity(?vectorA, ?vectorB) AS ?dist)
                                    FILTER(?productA != ?productB && ?dist < 1.0)
                    } ORDER BY DESC(?dist)
                    LIMIT 10
                """

import os
from pathlib import Path
import logging

from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from .base_dataset import (
    BaseDataset,
    DataTensor,
    QUERY_DIFFICULTY,
    QUERY_TYPE,
    mixin_queries,
)
import subprocess
import shutil
import rdflib


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BerlinSparqlBenchmark(
    mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.EMBEDDED],
    mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.EMBEDDED],
    mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.INDEX],
    mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.INDEX],
    mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.TWO_STAGE],
    mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.TWO_STAGE],
    BaseDataset,
):
    def __init__(
        self,
        base_dir: Path = Path("./bsbm"),
        ttl_file: Path = "dataset.nt",
        n=int(1e9),
        use_docker: bool = True,
        bsbm_directory: Path = Path("./data/bsbmtools-0.2"),
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
        self.use_docker = use_docker
        self.bsbm_directory = Path(bsbm_directory)

    def setup(self):
        if self.full_ttl_file.exists():
            logger.info(
                f"BSBM dataset already exists in {self.base_dir}, skipping generation"
            )
            return

        logger.info(f"Generating BSBM dataset in {self.base_dir}")
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.use_docker:
            subprocess.run(
                f"docker run -v {self.base_dir.absolute()}:/app/data -e 'DATA_DESTINATION=./' vcity/bsbm generate -pc {self.n}",
                shell=True,
                check=True,
            )
        else:
            subprocess.run(
                f"./generate -pc {self.n}",
                shell=True,
                check=True,
                cwd=self.bsbm_directory,
            )
            os.rename(self.bsbm_directory / "dataset.nt", self.full_ttl_file)

    def get_ttl_files(self):
        return [self.full_ttl_file]

    def get_encoded_ttl_files(self):
        return [self.base_dir / f"{self.full_ttl_file.stem}_encoded.nt"]

    def get_ttl_file(self):
        return self.full_ttl_file

    def get_encoded_ttl_file(self):
        return self.base_dir / f"{self.full_ttl_file.stem}_encoded.nt"

    def get_query_easy_index(self, embedding: DataTensor) -> str:
        return f"""
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX tensorIndex: <https://qlever.cs.uni-freiburg.de/tensorIndex/>
SELECT DISTINCT ?product  ?vector ?dist
WHERE {{
SERVICE tensorIndex: {{
       _:config tensorIndex:numNN 10 ;
      tensorIndex:left ?query_vector ;
      tensorIndex:bindDistance ?dist ;
      tensorIndex:payload ?product ;
      tensorIndex:searchK 1;
      tensorIndex:algorithm tensorIndex:ivf;
    # tensorIndex:experimentalRightCacheName "easy_index_bsbm" ;
      tensorIndex:right ?vector .
       {{
            {{
                SELECT DISTINCT ?product ?vector WHERE {{
                    ?product rdf:label_embedding ?vector .
                    ?product bsbmv:productFeature ?feat .
                }} GROUP BY ?product ?vector
            }}
        }}
    }}
    VALUES (?query_vector) {{ ({embedding.to_literal().n3()}) }}
}} ORDER BY DESC(?dist)
LIMIT 10
"""

    def get_query_hard_index(self, embedding: DataTensor) -> str:
        return f"""
PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX tensorIndex: <https://qlever.cs.uni-freiburg.de/tensorIndex/>
SELECT DISTINCT ?productA ?productB ?dist ?vectorA ?vectorB
WHERE {{
?productA bsbmv:productFeature ?featureA .
?featureA rdf:comment_embedding ?vectorA .
SERVICE tensorIndex: {{
    _:config tensorIndex:numNN 1 ;
    tensorIndex:left ?vectorA ;
    tensorIndex:bindDistance ?dist ;
    tensorIndex:payload ?productB, ?featureB ;
    tensorIndex:experimentalRightCacheName "hard_index_bsbm" ;
    tensorIndex:searchK 1 ;
    tensorIndex:right ?vectorB .
    {{
                ?productB bsbmv:productFeature ?featureB .
                ?featureB rdf:comment_embedding ?vectorB .
        }}
    }}
    VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}
}} ORDER BY DESC(?dist)
LIMIT 10
"""

    def get_query_easy_embedded(
        self,
        embedding: DataTensor,
    ) -> str:
        return f"""
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
SELECT DISTINCT ?product  ?vector ?dist 
WHERE {{
?product rdf:label_embedding ?vector .
?product bsbmv:productFeature ?feat .
BIND(dtf:dotProduct(?vector, {embedding.to_literal().n3()}) AS ?dist) .
}} GROUP BY ?product ?vector ?dist
ORDER BY DESC(?dist)
LIMIT 10
"""

    def get_query_hard_embedded(self, embedding: DataTensor) -> str:
        return f"""
            PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
            PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            SELECT DISTINCT ?productA ?productB ?dist ?vectorA ?vectorB
            WHERE {{
                ?featureA rdf:comment_embedding ?vectorA .
                ?productA bsbmv:productFeature ?featureA .
                {{
                    SELECT DISTINCT ?productB ?featureB ?vectorB WHERE {{
                         ?productB bsbmv:productFeature ?featureB .
                         ?featureB rdf:comment_embedding ?vectorB .
                    }}    
                }}
                BIND(dtf:dotProduct(?vectorA, ?vectorB) AS ?dist)
                FILTER(?productA != ?productB && ?featureA != ?featureB && ?dist < 1.0)
                VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}
            }} ORDER BY DESC(?dist)
            LIMIT 10
        """

    def get_query_easy_two_stage(self, embedding: DataTensor, limit=100000) -> str:
        return f"""
            PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
            PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            SELECT DISTINCT ?product  ?vector ?feat ?dist
            WHERE {{
                ?productA bsbmv:productFeature ?feat .
                ?product rdf:label_embedding ?vector .
                VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}
            }}
            LIMIT {limit}
        """

    def get_query_hard_two_stage(self, embedding: DataTensor, limit=100000) -> str:
        return f"""
            PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
            PREFIX bsbmi: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbmv: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            SELECT DISTINCT ?productA ?productB ?vectorA ?vectorB
            WHERE {{
                ?productA bsbmv:productFeature ?featureA .
                ?featureA rdf:comment_embedding ?vectorA .
                ?productB bsbmv:productFeature ?featureB .
                ?featureB rdf:comment_embedding ?vectorB .
                VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}
            }}
            LIMIT {limit}
        """

    def get_estimated_size(self) -> int:
        return self.n

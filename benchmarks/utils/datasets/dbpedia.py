from pathlib import Path

from sentence_transformers import SentenceTransformer

from .base_dataset import (
    BaseDataset,
    QUERY_DIFFICULTY,
    QUERY_TYPE,
    mixin_queries,
    DataTensor,
)


class DBPedia(
    mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.EMBEDDED],
    mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.EMBEDDED],
    mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.INDEX],
    mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.INDEX],
    # mixin_queries[QUERY_DIFFICULTY.EASY][QUERY_TYPE.TWO_STAGE],
    # mixin_queries[QUERY_DIFFICULTY.HARD][QUERY_TYPE.TWO_STAGE],
    BaseDataset,
):
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

    def get_query_easy_embedded(self, embedding: DataTensor) -> str:
        return f"""
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
SELECT * WHERE {{
    ?s a dbo:Ship .
    ?s dbo:thumbnail_embedding ?thumb_emb .
    ?s dbo:thumbnail_original ?thumb .
    BIND(dtf:cosineSimilarity(?thumb_emb, {embedding.to_literal().n3()}) AS ?dist)
}} 
ORDER BY DESC(?dist)
LIMIT 5
        """

    def get_query_easy_index(self, embedding: DataTensor) -> str:
        return f"""
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>
SELECT * WHERE {{
SERVICE tensorSearch: {{
    _:config tensorSearch:numNN 10 ;
    tensorSearch:left ?query_vector ;
    tensorSearch:bindDistance ?dist ;
    tensorSearch:payload ?s, ?thumb ;
    tensorSearch:searchK 1 ;
    tensorSearch:nTrees 512 ;
    # tensorSearch:experimentalRightCacheName "easy_index_dbpedia" ;
    tensorSearch:right ?thumb_emb ;
    tensorSearch:algorithm tensorSearch:faiss ;
    tensorSearch:distance tensorSearch:dot .
       {{
            ?s a dbo:Ship ;
            dbo:thumbnail_embedding ?thumb_emb ;
            dbo:thumbnail_original ?thumb .
        }}
    }}
    VALUES (?query_vector) {{ ({embedding.to_literal().n3()}) }}
}} 
ORDER BY DESC(?dist)
        """

    def get_query_hard_embedded(self, embedding: DataTensor) -> str:
        return f"""
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
SELECT DISTINCT ?r ?s ?dist ?thumb_rail_emb ?thumb_ship_emb WHERE {{
    ?s dbo:thumbnail_embedding ?thumb_ship_emb .
    ?s a dbo:Ship .
    {{
        SELECT DISTINCT ?s ?r ?thumb_rail_emb  WHERE {{
            ?r a dbo:RailwayLine .
            ?r dbo:thumbnail_embedding ?thumb_rail_emb .
        }}
    }}

    BIND(dtf:dotProduct(?thumb_ship_emb, ?thumb_rail_emb) AS ?dist) .
    VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}
}}ORDER BY DESC(?dist)"""

    def get_query_hard_index(self, embedding: DataTensor) -> str:
        return f"""
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>
SELECT DISTINCT ?r ?s  ?dist ?thumb_rail_emb ?thumb_ship_emb WHERE {{
    {{
    SELECT DISTINCT ?r ?s ?dist ?thumb_rail_emb ?thumb_ship_emb WHERE {{
        
        ?r a dbo:RailwayLine .
        ?r dbo:thumbnail_embedding ?thumb_rail_emb .
        ?r dbo:thumbnail_original ?thumb_rail_original .
                                    
        SERVICE tensorSearch: {{
        _:config tensorSearch:numNN 1 ;
        tensorSearch:left ?thumb_rail_emb ;
        tensorSearch:bindDistance ?dist ;
        tensorSearch:payload ?s, ?thumb_ship ;
        tensorSearch:searchK 1 ;
        tensorSearch:nTrees 512 ;
        tensorSearch:experimentalRightCacheName "hard_index_dbpedia" ;
        tensorSearch:right ?thumb_ship_emb ;
        tensorSearch:algorithm tensorSearch:faiss ;
        tensorSearch:distance tensorSearch:dot .
        {{
            ?s a dbo:Ship ;
            dbo:thumbnail_embedding ?thumb_ship_emb ;
            dbo:thumbnail_original ?thumb_ship .            
        }}
        }}                  
    }}
    }}
    VALUES (?some_emb) {{ ({embedding.to_literal().n3()}) }}         
}}
ORDER BY DESC(?dist)
LIMIT 20
        """

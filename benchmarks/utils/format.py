import pandas as pd


def map_df_readable(df: pd.DataFrame) -> pd.DataFrame:
    engine_mapping = {
        "Fuseki + RDFTensor": "RDFTensor",
        "Fuseki": "RDFTensor",
        "Qlever": "\\systemname-TV",
        "Qlever (no Tensor Vocabulary)": "\\systemname-Base",
        "QLever": "\\systemname-TV",
        "QLever (no Tensor Vocabulary)": "\\systemname-Base",
        "Neo4j": "Neo4j",
    }
    query_type_mapping = {
        "cypher_embedded": "embedded",
        "cypher_index": "index",
    }
    type_map = {
        "index": "Index",
        "embedded": "Embedded",
        "two_stage": "Two Stage",
    }
    if "engine" in df.columns:
        df["engine"] = df["engine"].map(engine_mapping).fillna(df["engine"])
    if "query_type" in df.columns:
        df["query_type"] = (
            df["query_type"].map(query_type_mapping).fillna(df["query_type"])
        )
        df["query_type"] = df["query_type"].map(type_map).fillna(df["query_type"])
    return df

# %%
from rdflib import Literal, URIRef

from utils.datasets.utils import parse_nt_to_generator, save_from_generator
from utils.datasets.dbpedia import DBPedia
from pathlib import Path
from utils.dbs.base_db import BaseDB
from utils.dbs.qlever_native import QleverDBNative
import pandas as pd
from PIL import Image

from sentence_transformers import SentenceTransformer
import argparse

# %%
from io import BytesIO
from tqdm import tqdm
import requests
from utils.datasets.data_tensor import DataTensor
import pyarrow.parquet as pq
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--dbpedia-dir", default="./data/dbpedia")
parser.add_argument(
    "--datafile", default=0, type=int, help="the parquet file id to process (0-330)"
)
parser.add_argument(
    "--batch-size", default=8, type=int, help="the batch size for encoding the images"
)
parser.add_argument(
    "--use-native",
    default=True,
    help="whether to use the native Qlever DB or the dockerized version",
    action="store_true",
)
parser.add_argument(
    "--model",
    default="clip-ViT-B-32",
    type=str,
    help="the model to use for encoding the images",
)
parser.add_argument(
    "--out-dir",
    default="./data/dbpedia/encoded_triples",
    type=str,
    help="the directory to save the encoded triples",
)
args = parser.parse_args()


out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)
repo_id = "wikimedia/wit_base"
parquet_file_id = 0
max_parquet_file_id = 330


def is_en(wit_feature: dict) -> bool:
    lang_list = (
        wit_feature["language"]
        if isinstance(wit_feature["language"], list)
        else wit_feature["language"].tolist()
    )
    return "en" in lang_list


def en_data(wit_feature: dict) -> dict:
    if is_en(wit_feature):
        # convert the ndarray to a list
        langs = (
            wit_feature["language"]
            if isinstance(wit_feature["language"], list)
            else wit_feature["language"].tolist()
        )
        en_idx = langs.index("en")
        return {k: v[en_idx] for k, v in wit_feature.items() if len(v) > en_idx}
    return None


def urls_in_dbpedia(rows: pd.DataFrame, db: BaseDB) -> dict[int, list[str]]:  #
    titles = {id: row["en_wit_features"]["page_title"] for id, row in rows.iterrows()}

    values = " ".join(f" ({id} {Literal(title).n3()})" for id, title in titles.items())

    subjs = db.query(f"""SELECT ?s ?id ?thumb ?title ?label WHERE {{
        ?s dbo:thumbnail  ?thumb.
        ?s rdfs:label ?label .
        VALUES (?id ?title) {{ {values} }}
        FILTER(CONTAINS(?label, ?title))
    }}""")
    # filter out only equal matches
    subjs = subjs[subjs.apply(lambda row: row["label"] == row["title"], axis=1)]

    subjs["id"] = subjs["id"].astype(int)
    return subjs


def encode_dbpedia_thumbnails(
    db: BaseDB,
    model: SentenceTransformer,
    df: pd.DataFrame = None,
    thumbnail_predicate: str = "http://dbpedia.org/ontology/thumbnail",
):
    # get all subjects from the dataframe
    df_dbpedia_subjects = urls_in_dbpedia(df, db)
    df_batch = df.loc[df_dbpedia_subjects["id"]]
    df_batch["dbpedia_subjects"] = df_batch.apply(
        lambda row: df_dbpedia_subjects[df_dbpedia_subjects["id"] == row.name][
            "s"
        ].tolist(),
        axis=1,
    )
    df_batch["image_pil"] = df_batch.apply(
        lambda row: Image.open(BytesIO(row["image"]["bytes"])), axis=1
    )
    # encode the images
    encodings = model.encode(
        df_batch["image_pil"].tolist(),
        batch_size=args.batch_size,
        show_progress_bar=True,
    )
    df_batch["encoding"] = [enc for enc in encodings]
    # generate triples and return them
    triples = [
        (
            URIRef(s.replace("dbr:", "http://dbpedia.org/resource/")),
            URIRef(f"{thumbnail_predicate}_embedding"),
            DataTensor.from_numpy(row["encoding"]).to_literal(),
        )
        for _, row in df_batch.iterrows()
        for s in row["dbpedia_subjects"]
    ]
    return triples


def prepare_df(df: pd.DataFrame):
    df = df[df["wit_features"].apply(is_en)]
    df["en_wit_features"] = df["wit_features"].apply(en_data)
    return df


def generate_triples_for_datafile_id(
    db: BaseDB, model: SentenceTransformer, datafile_id: int
):
    repo_id = "wikimedia/wit_base"
    max_parquet_file_id = 330
    path_in_repo = f"data/train-{datafile_id:05d}-of-{max_parquet_file_id:05d}.parquet"
    with pq.ParquetFile(f"hf://datasets/{repo_id}/{path_in_repo}") as pf:
        for rg in tqdm(
            range(pf.num_row_groups),
            desc=f"Processing datafile {datafile_id:05d} over groups",
        ):
            df = pf.read_row_group(rg).to_pandas()
            df = prepare_df(df)
            # split the dataframe into batches and encode the image for each batch
            with open(
                out_dir
                / f"dbpedia_thumbnails_embeddings_{datafile_id:05d}_{rg:05d}.nt",
                "w",
            ) as f:
                for batch_start in tqdm(
                    range(0, len(df), args.batch_size),
                    desc=f"Processing batches for group {rg:05d}",
                ):
                    batch_end = min(batch_start + args.batch_size, len(df))
                    df_batch = df.iloc[batch_start:batch_end]
                    try:
                        triples = encode_dbpedia_thumbnails(db, model, df=df_batch)
                        for s, p, o in triples:
                            f.write(f"{s.n3()} {p.n3()} {o.n3()} .\n")
                        f.flush()
                    except TimeoutError as e:
                        print(f"Error occurred while processing batch: {e}")


# %%

if __name__ == "__main__":
    # %%

    model = SentenceTransformer(args.model)
    dataset = DBPedia(base_dir=Path(args.dbpedia_dir))
    db = QleverDBNative(
        dataset=dataset,
        id="dbpedia",
        name="QleverDB-DBPedia",
        base_dir=Path(args.dbpedia_dir),
        use_encoded_ttl=False,
        port_offset=args.datafile,
    )
    db.db_dir = Path(args.dbpedia_dir)

    # %%
    db.setup()
    generate_triples_for_datafile_id(db, model, args.datafile)

from PIL import Image
from io import BytesIO
import requests

import base64
from IPython.display import HTML

import pandas as pd
import numpy as np
import time
import tqdm
from pathlib import Path
import logging
import hashlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
USER_AGENT = "Qlever Image Viewer/1.1"


def get_wc_thumb(image: str, width=330):  # image = e.g. from Wikidata, width in pixels
    image_name = image.split("/")[-1]
    logger.info(f"Building thumbnail for {image_name}...")
    image_name = image_name.replace(" ", "_")  # need to replace spaces with underline
    m = hashlib.md5()
    m.update(image_name.encode("utf-8"))
    d = m.hexdigest()
    return (
        "https://upload.wikimedia.org/wikipedia/commons/thumb/"
        + d[0]
        + "/"
        + d[0:2]
        + "/"
        + image_name
        + "/"
        + str(width)
        + "px-"
        + image_name
    )


def thumbs_to_pil(thumbs: pd.Series):

    pil_images = []
    g = tqdm.tqdm(thumbs, desc="Fetching thumbnails")
    for thumb in g:
        g.set_description(f"Fetching thumbnail for {thumb}...")
        # url_thumb = get_wc_thumb(thumb)
        thumb_url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{thumb.split('/')[-1]}?width=330"
        try:
            response = None
            while response is None or response.status_code == 429:  # Too Many Requests
                response = requests.get(
                    thumb_url,
                    headers={"User-Agent": USER_AGENT},
                )
                if response.status_code == 429:
                    raise ValueError("Rate limited")
                    waiting_time = response.headers.get("Retry-After", 5)
                    logger.warning(
                        f"Rate limited when fetching {thumb_url}, retrying after {waiting_time} seconds..."
                    )
                    time.sleep(int(waiting_time))
            img = Image.open(BytesIO(response.content))
            pil_images.append(img)
        except Exception as e:
            logger.error(f"Error loading image {thumb_url}: {e}")
            pil_images.append(None)
    return pd.Series(pil_images, index=thumbs.index)


# https://stackoverflow.com/a/55194315
def image_base64(im: Image):
    with BytesIO() as buffer:
        if im.mode in ("RGBA", "P"):
            im = im.convert("RGB")
        im.save(buffer, "jpeg")
        return base64.b64encode(buffer.getvalue()).decode()


def image_formatter(im):
    return f'<img src="data:image/jpeg;base64,{image_base64(im)}" width="300px">'

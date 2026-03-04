from pathlib import Path
import logging
from .base_dataset import BaseDataset
import subprocess
import shutil


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BerlinSparqlBenchmark(BaseDataset):
    def __init__(self, base_dir: Path = Path("./bsbm"), n=int(1e9)):
        self.base_dir = base_dir
        self.full_ttl_file = self.base_dir / "_complete.ttl"

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
            f"docker run -v {self.base_dir}:/app/data -e 'DATA_DESTINATION=/app/data/_complete.ttl' vcity/bsbm generate-n ",
            shell=True,
            check=True,
        )

    def get_ttl_files(self):
        return [self.full_ttl_file]

    def get_ttl_file(self):
        return self.full_ttl_file

    def get_triples(self):
        pass

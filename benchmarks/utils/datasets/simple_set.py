from pathlib import Path

from .base_dataset import BaseDataset
import subprocess
import shutil


class SimpleSet(BaseDataset):
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.full_ttl_file = self.base_dir / "_complete.ttl"

    def setup(self):
        pass

    def get_ttl_files(self):
        return list(self.base_dir.rglob("*.ttl"))

    def get_ttl_file(self):
        if self.full_ttl_file.exists():
            return self.full_ttl_file
        else:
            # concatenate all ttl files into one using jena's riot tool
            ttl_files = self.get_ttl_files()
            if not ttl_files:
                raise ValueError(f"No ttl files found in {self.base_dir}")
            output_file = self.full_ttl_file
            subprocess.run(
                f"riot {' '.join(str(f) for f in ttl_files)} > {output_file}",
                shell=True,
                check=True,
            )
            return output_file

    def get_triples(self):
        pass

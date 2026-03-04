from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any
import os


class BaseDataset(ABC):
    """Abstract base class for datasets."""

    def __init__(self, name: str, data_dir: Optional[str] = None):
        """
        Initialize the dataset.

        Args:
            name: Name of the dataset
            data_dir: Directory to store/load dataset files
        """
        self.name = name
        self.data_dir = data_dir or os.getcwd()
        self._data = None

    def setup(self):
        pass

    def get_ttl_files(self):
        return list(self.base_dir.rglob("*.ttl"))

    def get_ttl_file(self):
        return self.base_dir / "_complet.ttl"

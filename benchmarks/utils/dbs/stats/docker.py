import json
import logging
import re
import subprocess
import threading
import traceback

import pandas as pd
from .base import BaseStatRecorder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DockerStatRecorder(BaseStatRecorder):
    def __init__(self, container_name: str):
        super().__init__(identifier=container_name)
        self.container_name = container_name
        self.stats_map = {
            "cpu_percent": "CPUPerc",
            "mem_usage": "MemUsage",
            "net_io": "NetIO",
            "block_io": "BlockIO",
            "pids": "PIDs",
        }
        self.stats_parsers = {
            "cpu_percent": lambda x: float(x.strip("%")),
            "mem_usage": lambda x: self.parse_datasize(x),
            "net_io": lambda x: self.parse_datasize(x),
            "block_io": lambda x: self.parse_datasize(x),
            "pids": lambda x: int(x),
        }
        logger.debug(
            f"Initialized StatRecorder for container {self.container_name} with stats_map: {self.stats_map} and stats_parsers: {self.stats_parsers}"
        )

    def parse_datasize(self, size_str: str) -> float:
        # size_str is in the format "123.45MB" or "1.23GB"

        used_str, total_str = size_str.split("/")

        try:
            size_str = used_str.strip().upper()
            multiplier = 1
            number = re.findall(r"^[\d\.]+", size_str)
            number = float(number[0]) if number else 0.0
            multiplier_s = re.findall(r"[KMGT][iI]?B", size_str)
            multiplier_s = multiplier_s[0] if multiplier_s else "B"
            base_multiplier = 1024 if multiplier_s.lower().endswith("ib") else 1000
            power = "KMGT".find(multiplier_s[0]) + 1 if multiplier_s[0] in "KMGT" else 0
            multiplier = base_multiplier**power

            logger.debug(
                f"Parsed size string '{size_str}' as number={number} and multiplier={multiplier}, base multiplier={base_multiplier}, power={power}"
            )
            return number * multiplier
        except Exception as e:
            logger.error(f"Error parsing data size string '{size_str}': {e}")
            return 0.0

    def record_stats(self):
        try:
            result = subprocess.run(
                f"docker stats {self.container_name} --no-stream --format 'json'",
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )
            output = json.loads(result.stdout.strip())
            logger.debug(
                f"Raw docker stats output for container {self.container_name}: {output}"
            )
            record = {
                "timestamp": pd.Timestamp.now(),
            } | {
                k: self.stats_parsers[k](output[ok] if ok is not None else None)
                for k, ok in self.stats_map.items()
            }
            self.add_stats(record)
        except Exception as e:
            logger.error(
                f"Error recording stats for container {self.container_name}: {e}"
            )
            traceback.print_exc()

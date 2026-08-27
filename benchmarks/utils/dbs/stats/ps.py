import logging
import threading
import traceback

import pandas as pd
import psutil as ps

from .base import BaseStatRecorder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PSStatRecorder(BaseStatRecorder):
    def __init__(self, pid: int):
        super().__init__(identifier=f"pid_{pid}")
        self.pid = pid
        self.stats_parsers = {
            "cpu_percent": lambda x: float(x.strip("%")),
            "mem_usage": lambda x: self.parse_datasize(x),
            "net_io": lambda x: self.parse_datasize(x),
            "block_io": lambda x: self.parse_datasize(x),
            "pids": lambda x: int(x),
        }
        logger.debug(f"Initialized StatRecorder for PID {self.pid}")
        self.stats_lock = threading.RLock()
        self.clear_stats()
        self.proc = ps.Process(self.pid)

    def get_wall_time(self):
        try:
            all_procs = [self.proc] + self.proc.children(recursive=True)
            full_wall_time = sum(
                p.cpu_times().user + p.cpu_times().system for p in all_procs
            )
            return full_wall_time
        except Exception as e:
            logger.error(f"Error getting wall time for PID {self.pid}: {e}")
            traceback.print_exc()
            return None

    def record_stats(self):
        try:
            proc = self.proc
            with proc.oneshot():
                all_procs = [proc] + proc.children(recursive=True)
                full_memory_usage = sum(p.memory_full_info().rss for p in all_procs)
                full_cpu_percent = sum(p.cpu_percent(interval=None) for p in all_procs)
                record = {
                    "timestamp": pd.Timestamp.now(),
                    "cpu_percent": full_cpu_percent,
                    "mem_usage": full_memory_usage,
                    "net_io": -1,
                    "block_io": -1,  # sum(proc.io_counters()),
                    "pids": len(proc.children(recursive=True)) + 1,
                }
                self.add_stats(record)
                logger.debug(f"Raw stats output for PID {self.pid}: {record}")
        except ps.NoSuchProcess:
            logger.warning(f"Process with PID {self.pid} no longer exists.")
            self.stop_recording()
        except Exception as e:
            logger.error(f"Error recording stats for container {self.pid}: {e}")
            traceback.print_exc()

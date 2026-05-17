from abc import abstractmethod

import logging
import threading

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseStatRecorder:
    def __init__(self, identifier: str):
        self.stats_lock = threading.RLock()
        self.clear_stats()

    def get_stats(self) -> pd.DataFrame:
        with self.stats_lock:
            return self.stats_recorded.copy()

    def add_stats(self, stats: dict):
        logger.debug(f"Adding stats: {stats}")
        with self.stats_lock:
            new_df = pd.DataFrame([stats])
            self.stats_recorded = pd.concat(
                [self.stats_recorded, new_df], ignore_index=True
            )

    @abstractmethod
    def record_stats(self) -> None:
        raise NotImplementedError("record_stats must be implemented by subclasses")

    @abstractmethod
    def get_wall_time(self) -> float:
        raise NotImplementedError("get_wall_time must be implemented by subclasses")

    def start_recording(self, interval_seconds=0.1):

        if hasattr(self, "_record_thread") and self._record_thread.is_alive():
            logger.warning(
                f"Stat recording already running for container {self.identifier}. Ignoring start_recording call."
            )
            return
        self._stop_event = threading.Event()

        def record_loop():
            while not self._stop_event.is_set():
                self.record_stats()
                self._stop_event.wait(interval_seconds)

        self.record_stats()  # Record initial stats immediately
        self._record_thread = threading.Thread(target=record_loop)
        self._record_thread.start()

    def stop_recording(self):
        if hasattr(self, "_stop_event") and hasattr(self, "_record_thread"):
            self._stop_event.set()
            self._record_thread.join()

    def clear_stats(self):
        with self.stats_lock:
            self.stats_recorded = pd.DataFrame(
                columns=[
                    "timestamp",
                    "cpu_percent",
                    "mem_usage",
                    "net_io",
                    "block_io",
                    "pids",
                ]
            )

import gc
import os
import sys
import time
import random
import asyncio
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_libc = None
if sys.platform.startswith("linux"):
    try:
        import ctypes
        _libc = ctypes.CDLL("libc.so.6")
    except Exception:
        _libc = None

def trim_memory() -> None:
    """
    Runs garbage collection and triggers malloc_trim on Linux
    to release unfragmented heap arenas directly back to the Linux OS.
    Critical on Pentium dual-core / 8GB RAM systems.
    """
    gc.collect()
    if _libc is not None and hasattr(_libc, "malloc_trim"):
        try:
            _libc.malloc_trim(0)
        except Exception:
            pass

class SystemHealthMonitor:
    """
    Non-blocking system resource monitor that ensures workers do not saturate the machine.
    Defaults to 75% for CPU and Memory as configured for Pentium G4400 / 8GB RAM machines.
    """

    def __init__(self, cpu_threshold: float = 75.0, mem_threshold: float = 75.0):
        self.cpu_threshold = cpu_threshold
        self.mem_threshold = mem_threshold
        import psutil
        self._psutil = psutil
        # Prime the CPU counter so non-blocking reads are accurate
        try:
            self._psutil.cpu_percent(interval=None)
        except Exception:
            pass

    def is_healthy(self) -> Tuple[bool, str]:
        try:
            cpu = self._psutil.cpu_percent(interval=None)
            if cpu > self.cpu_threshold:
                return False, f"CPU too high ({cpu}%)"

            mem = self._psutil.virtual_memory().percent
            if mem > self.mem_threshold:
                trim_memory()
                mem_after = self._psutil.virtual_memory().percent
                if mem_after > self.mem_threshold:
                    return False, f"Memory too high ({mem_after}%)"
            return True, "Healthy"
        except Exception as e:
            logger.debug(f"Error checking system health: {e}")
            return True, "Healthy"

    async def wait_for_resources(self, log_handler: Optional[logging.Logger] = None, timeout: Optional[float] = 60.0) -> None:
        """
        Pauses execution with micro-pauses (0.5s - 1.2s) if CPU or RAM exceeds 75%.
        Ensures smooth desktop operation without OOM or freezing.
        """
        start = time.time()
        active_logger = log_handler or logger

        while True:
            healthy, reason = self.is_healthy()
            if healthy:
                break

            if timeout is not None and (time.time() - start) > timeout:
                active_logger.warning(f"SystemHealthMonitor: Max wait reached ({reason}). Proceeding with task.")
                break

            active_logger.warning(f"SystemHealthMonitor: 75% threshold reached ({reason}). Applying micro-backoff...")
            if "CPU" in reason:
                await asyncio.sleep(random.uniform(0.5, 1.2))
            else:
                trim_memory()
                await asyncio.sleep(random.uniform(1.5, 3.0))

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time


class SizeAndTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Rotate the log file at certain time intervals (inherited) OR when the file
    exceeds maxBytes. This extends TimedRotatingFileHandler by adding a size check.

    Parameters:
    - filename: path to the log file
    - when, interval, backupCount: passed to TimedRotatingFileHandler
    - maxBytes: rotate when file size exceeds this (0 = disabled)
    """

    def __init__(self, filename, when='midnight', interval=1, backupCount=7, encoding=None, delay=False, utc=False, maxBytes=0):
        super().__init__(filename, when=when, interval=interval, backupCount=backupCount, encoding=encoding, delay=delay, utc=utc)
        self.maxBytes = int(maxBytes or 0)

    def shouldRollover(self, record):
        # First check the timed rollover condition
        try:
            t_roll = super().shouldRollover(record)
        except Exception:
            t_roll = False

        if t_roll:
            return 1

        # Then check the size-based rollover
        if self.maxBytes > 0:
            try:
                if self.stream is None:
                    self.stream = self._open()
                self.stream.flush()
                if os.path.exists(self.baseFilename) and os.path.getsize(self.baseFilename) >= self.maxBytes:
                    return 1
            except Exception:
                # If any problem checking size, do not prevent logging
                return 0

        return 0


def ensure_logs_dir(base_path: str | None = None):
    """Return the path to the logs directory, creating it if necessary.

    If base_path is None, the logs directory is created next to this package (../logs).
    """
    if base_path:
        logs_dir = os.path.normpath(base_path)
    else:
        logs_dir = os.path.normpath(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'logs'))
    os.makedirs(logs_dir, exist_ok=True)
    return logs_dir

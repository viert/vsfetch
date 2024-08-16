import os
import sys
import tomllib
import logging
from logging import Logger
from typing import Optional
from .config import Config


DEFAULT_CONFIG_FILENAME = "/etc/vsfetch/vsfetch.toml"


class Context:

    _cfg_filename: Optional[str] = None
    _cfg: Optional[Config] = None
    _log: Optional[Logger] = None

    def set_config_filename(self, filename: str):
        if self._cfg is not None:
            self._cfg = None
        self._cfg_filename = filename

    @property
    def cfg_filename(self) -> str:
        if self._cfg_filename is None:
            self._cfg_filename = os.getenv("APP_CONFIG", DEFAULT_CONFIG_FILENAME)
        return self._cfg_filename

    @property
    def cfg(self) -> Config:
        if self._cfg is None:
            try:
                with open(self.cfg_filename, "rb") as f:
                    attrs = tomllib.load(f)
                    self._cfg = Config(**attrs)
            except EnvironmentError as e:
                self.log.error(f"error reading config: {e}, using defaults")
                self._cfg = Config()
        return self._cfg

    @property
    def log(self) -> Logger:
        if self._log is None:
            log = logging.getLogger(__name__)
            log.propagate = False
            log.setLevel(logging.DEBUG)

            for handler in log.handlers:
                log.removeHandler(handler)

            formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(formatter)

            log.addHandler(handler)
            self._log = log
        return self._log


ctx = Context()

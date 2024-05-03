import tomllib
from pydantic import BaseModel, Field
from typing import Optional
from vsfetch.log import log


class TrackedConfig(BaseModel):
    base_url: str = "http://localhost:9441"
    timeout: float = 3.0


class VersionedConfig(BaseModel):
    base_url: str = "http://localhost:9440"
    timeout: float = 3.0


class ExternalConfig(BaseModel):
    timeout: float = 3.0


class Config(BaseModel):
    tracked: TrackedConfig = Field(default_factory=TrackedConfig)
    versioned: VersionedConfig = Field(default_factory=VersionedConfig)
    external: ExternalConfig = Field(default_factory=ExternalConfig)


def load(filename: str) -> Config:
    with open(filename, "rb") as f:
        attrs = tomllib.load(f)
        return Config(**attrs)


_cfg: Optional[Config] = None


def init_config(filename: str) -> None:
    global _cfg
    try:
        _cfg = load(filename)
    except EnvironmentError:
        log.error(f"error loading config {filename}, using defaults")
        _cfg = Config()


def get_config() -> Config:
    global _cfg
    if _cfg is None:
        raise RuntimeError("config is not initialized yet")
    return _cfg

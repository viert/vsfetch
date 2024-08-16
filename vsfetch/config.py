from pydantic import BaseModel, Field
from typing import Dict, Any


class TrackedConfig(BaseModel):
    base_url: str = "http://localhost:9441"
    timeout: float = 3.0


class VersionedConfig(BaseModel):
    base_url: str = "http://localhost:9440"
    timeout: float = 3.0


class ExternalConfig(BaseModel):
    timeout: float = 3.0


class DatabaseConfig(BaseModel):
    uri: str = "mongodb://127.0.0.1:27017/simwatch"
    kwargs: Dict[str, Any] = Field(default_factory=dict)


class Config(BaseModel):
    tracked: TrackedConfig = Field(default_factory=TrackedConfig)
    versioned: VersionedConfig = Field(default_factory=VersionedConfig)
    external: ExternalConfig = Field(default_factory=ExternalConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

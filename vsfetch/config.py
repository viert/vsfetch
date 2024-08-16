from pydantic import BaseModel, Field


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

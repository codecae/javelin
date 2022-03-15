from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class ComponentMetadata:
    name: str = None
    schema_hash: str = None
    object_format: str = None
    object_namespace: str = None
    object_name: str = None
    object_attributes: list = field(default_factory=list)

@dataclass
class ComponentMessage:
    header: ComponentMetadata
    content: dict = field(default_factory=dict)
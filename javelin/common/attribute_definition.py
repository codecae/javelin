from dataclasses import dataclass

@dataclass
class AttributeDefinition:
    name: str = ""
    datatype: type = None
    nullable: bool = True
    ref: any = None

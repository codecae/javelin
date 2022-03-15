from dataclasses import dataclass, field

@dataclass
class SqlConnection:
    drivername: str = ""
    username: str = ""
    password: str = ""
    host: str = ""
    port: int = None
    database: str = ""
    driver_options: dict = field(default_factory=dict)

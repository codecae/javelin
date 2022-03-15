from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.engine import URL
import pandas as pd
import datetime
from rx import from_iterable
from .source import SourceComponent
from .sql_connection import SqlConnection
from ..common import ComponentMetadata, ComponentMessage, AttributeDefinition

class SqlTableSource(SourceComponent):
    def __init__(self, config: SqlConnection):
        self._config = config
        self._table = None
        self._columns = None

    def _engine(self):
        _eng = create_engine(
            URL.create(
                drivername=self._config.drivername,
                host=self._config.host,
                port=self._config.port,
                username=self._config.username,
                password=self._config.password,
                database=self._config.database,
                query=self._config.driver_options
            )
        )
        yield _eng
        _eng.dispose()

    def _connection(self):
        for _eng in self._engine():
            _conn = _eng.connect().execution_options(stream_results=True)
            yield _conn
            _conn.close()
        

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def schema_name(self):
        return self._schema_name

    @schema_name.setter
    def schema_name(self, value):
        self._schema_name = value

    def _reflection(self) -> ComponentMetadata:
        for _engine in self._engine():
            _md = MetaData(bind=_engine, schema=self._schema_name)
            self._table = Table(self.table_name, _md, autoload_with=_engine)
            self._columns = [c for c in self._table.columns.values() if str(c.type) != 'NULL']
            yield ComponentMetadata(
                name=_engine.url.host,
                object_namespace=_engine.url.database,
                object_name=self._table.fullname,
                object_attributes=[AttributeDefinition(name=c.name, datatype=c.type.python_type, nullable=c.nullable, ref=c) for c in self._columns]
            )

    def yield_per(self, partition_rows: int = 10000, yield_rows: int = 10000) -> list:
        for _reflection in self._reflection():
            for _connection in self._connection():
                _map_cols = {c.name: "Int64" for c in self._columns if c.type.python_type == int}
                _select = select(self._columns)
                _results = _connection.execute(_select).yield_per(yield_rows)
                for _partition in _results.partitions(partition_rows):       
                    _df = pd.DataFrame(_partition).astype(_map_cols)
                    _reflection.object_format=type(_df)
                    yield ComponentMessage(
                        header=_reflection,
                        content=_df
                    )

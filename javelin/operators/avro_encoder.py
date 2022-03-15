import decimal
import datetime
import rx
import json
import pandas
from io import BytesIO
from fastavro import writer, parse_schema
from enum import Enum

PYTHON_AVRO_TYPE_MAP = {
    int: 'int',
    str: 'string',
    decimal.Decimal: 'float',
    datetime.datetime: 'string',
    datetime.date: 'string',
    bool: 'boolean',
    bytes: 'bytes'
}

class AvroCompressionType(Enum):
    DEFLATE = 'deflate'
    SNAPPY = 'snappy'

def avro_encoder(compressionType: AvroCompressionType):
    def _avro_encoder(source):
        def subscribe(observer, scheduler = None):            
            def on_next(value):
                if value.header.object_format != pandas.core.frame.DataFrame:
                    raise TypeError("Input message format for avro_encoder must be 'DataFrame'")
                _bytes = BytesIO()
                _schema = {
                    "namespace": f"{value.header.object_namespace}.avro",
                    "type": "record",
                    "name": value.header.object_name,
                    "fields": []
                }
                for attr in value.header.object_attributes:
                    if PYTHON_AVRO_TYPE_MAP[attr.datatype]:
                        _datatype = PYTHON_AVRO_TYPE_MAP[attr.datatype]
                    else:
                        _datatype = str(attr.datatype)
                    _schema["fields"].append({
                        "name": attr.name,
                        "type": (_datatype, [_datatype, "null"])[attr.nullable]
                    })
                value.content = [(lambda z: {k:(v.isoformat() if type(v)==pandas.Timestamp else v) for k,v in z.items() if pandas.notnull(v)})(z) for z in value.content.to_dict('records')]
                writer(_bytes, parse_schema(_schema), value.content, codec=compressionType.value)
                value.header.object_format = 'avro'
                value.content = _bytes.getvalue()
                observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler
            )
        return rx.create(subscribe)
    return _avro_encoder
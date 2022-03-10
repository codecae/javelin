import rx
import hashlib
import json
import pandas
from ..common import AttributeDefinition

def add_hash():
    def _add_hash(source):
        def subscribe(observer, scheduler = None):            
            def on_next(value):
                _key = '_hash'
                if value.header.object_format != pandas.core.frame.DataFrame:
                    raise TypeError("Input message format for add_hash must be 'DataFrame'")
                value.content[_key] = value.content.apply(lambda x: hashlib.sha256(bytes(str(tuple(x)),'utf-8')).hexdigest(), axis=1)
                value.header.object_attributes.append(AttributeDefinition(name=_key, datatype=str, nullable=False, ref=None))
                observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler
            )
        return rx.create(subscribe)
    return _add_hash
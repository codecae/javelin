from io import BytesIO
import rx
import pandas
import json

def json_encoder():
    def _json_encoder(source):
        def subscribe(observer, scheduler = None):            
            def on_next(value):
                if value.header.object_format != pandas.core.frame.DataFrame:
                    raise TypeError("Input message format for avro_encoder must be 'DataFrame'")
                value.content = json.loads(value.content.to_json(orient='records', date_format='iso'))
                value.content = json.dumps([(lambda z: {k:v for k,v in z.items() if v != None})(z) for z in value.content])
                value.header.object_format = 'json'
                observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler
            )
        return rx.create(subscribe)
    return _json_encoder
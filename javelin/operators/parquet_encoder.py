from io import BytesIO
import rx
import pandas

def parquet_encoder():
    def _parquet_encoder(source):
        def subscribe(observer, scheduler = None):            
            def on_next(value):
                if value.header.object_format != pandas.core.frame.DataFrame:
                    raise TypeError("Input message format for avro_encoder must be 'DataFrame'")
                _bytes = BytesIO()
                pandas.DataFrame(value.content).to_parquet(engine='pyarrow',path=_bytes)
                value.header.object_format = 'parquet'
                value.content = _bytes.getvalue()
                observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler
            )
        return rx.create(subscribe)
    return _parquet_encoder
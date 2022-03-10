from ..common import PipelineComponent

class SourceComponent(PipelineComponent):
    @property
    def input_data(self) -> list:
        raise NotImplementedError("Not Implemented on SourceComponent")
    
    @input_data.setter
    def input_data(self, data: dict):
        raise NotImplementedError("Not Implemented on SourceComponent")

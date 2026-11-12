class InvalidPipelineRunStatusError(Exception):
    """Raised when a pipeline run is not in 'queued' status"""
    pass

class UnknownStepTypeError(Exception):
    """Raised when a pipeline step type doesn't have a corresponding execution function"""
    pass

class IngestionStepError(Exception):
    """Raised when ingestion step fails"""
    pass

class ValidationStepError(Exception):
    """Raised when the validation step fails"""
    pass

class TransformationStepError(Exception):
    """Raised when the transformation step fails"""
    pass

class LoadStepError(Exception):
    """Raised when the load step fails"""
    pass

class ObservabilityRecordingError(Exception):
    """Raised when the observability recording step fails"""
    pass

class LeaseLostError(Exception):
    """Raised when this worker no longer owns the run: its lease ran out and another worker took the run over"""
    pass

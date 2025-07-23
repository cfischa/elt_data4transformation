"""
Custom exception classes for the BnB Data4Transformation project.
"""


class BnBDataException(Exception):
    """Base exception for all BnB Data4Transformation related errors."""
    pass


class ConnectorException(BnBDataException):
    """Raised when a connector encounters an error."""
    pass


class AuthenticationException(ConnectorException):
    """Raised when authentication fails."""
    pass


class APIRateLimitException(ConnectorException):
    """Raised when API rate limit is exceeded."""
    pass


class DataValidationException(BnBDataException):
    """Raised when data validation fails."""
    pass


class LoaderException(BnBDataException):
    """Raised when data loading fails."""
    pass


class ConfigurationException(BnBDataException):
    """Raised when configuration is invalid."""
    pass


class PipelineException(BnBDataException):
    """Raised when pipeline execution fails."""
    pass

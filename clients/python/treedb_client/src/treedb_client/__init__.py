"""Python client for TreeDB's pre-alpha document service."""

from .client import TreeDBClient
from .errors import (
    ConflictError,
    IndexNotFoundError,
    IndexStaleError,
    IndexUnavailableError,
    InternalServiceError,
    InvalidRequestError,
    MalformedJSONError,
    TreeDBClientError,
    TreeDBConfigError,
    TreeDBProtocolError,
    TreeDBServiceError,
    TreeDBTimeoutError,
    TreeDBTransportError,
    UnsupportedError,
)
from .filters import Filter, InvalidFilterError, normalize_filter
from .models import (
    CountDocumentsResponse,
    DeleteDocumentsResponse,
    DenseVectorSearchResponse,
    Document,
    FilterDocumentsResponse,
    IndexCapabilities,
    IndexInfo,
    UpsertDocumentsResponse,
)

__all__ = [
    "ConflictError",
    "CountDocumentsResponse",
    "DeleteDocumentsResponse",
    "DenseVectorSearchResponse",
    "Document",
    "Filter",
    "FilterDocumentsResponse",
    "IndexCapabilities",
    "IndexInfo",
    "IndexNotFoundError",
    "IndexStaleError",
    "IndexUnavailableError",
    "InternalServiceError",
    "InvalidFilterError",
    "InvalidRequestError",
    "MalformedJSONError",
    "TreeDBClient",
    "TreeDBClientError",
    "TreeDBConfigError",
    "TreeDBProtocolError",
    "TreeDBServiceError",
    "TreeDBTimeoutError",
    "TreeDBTransportError",
    "UnsupportedError",
    "UpsertDocumentsResponse",
    "normalize_filter",
]

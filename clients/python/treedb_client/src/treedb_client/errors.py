"""Exception hierarchy for the TreeDB document service client."""

from __future__ import annotations

from typing import Dict, Optional, Type


class TreeDBClientError(Exception):
    """Base class for all TreeDB Python client exceptions."""


class TreeDBConfigError(TreeDBClientError, ValueError):
    """Raised when client configuration is invalid."""


class TreeDBTransportError(TreeDBClientError):
    """Raised when the client cannot reach the TreeDB service."""


class TreeDBTimeoutError(TreeDBTransportError, TimeoutError):
    """Raised when a TreeDB service request times out."""


class TreeDBProtocolError(TreeDBClientError):
    """Raised when the service response does not match the HTTP/JSON contract."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class TreeDBServiceError(TreeDBClientError):
    """Base class for structured errors returned by the TreeDB service."""

    code = "internal"

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
    ) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.response_body = response_body


class InvalidRequestError(TreeDBServiceError):
    code = "invalid_request"


class MalformedJSONError(TreeDBServiceError):
    code = "malformed_json"


class IndexNotFoundError(TreeDBServiceError):
    code = "index_not_found"


class IndexUnavailableError(TreeDBServiceError):
    code = "index_unavailable"


class IndexStaleError(TreeDBServiceError):
    code = "index_stale"


class SnapshotMismatchError(TreeDBServiceError):
    code = "snapshot_mismatch"


class ConflictError(TreeDBServiceError):
    code = "conflict"


class UnsupportedError(TreeDBServiceError):
    code = "unsupported"


class InternalServiceError(TreeDBServiceError):
    code = "internal"


ERROR_CLASS_BY_CODE: Dict[str, Type[TreeDBServiceError]] = {
    InvalidRequestError.code: InvalidRequestError,
    MalformedJSONError.code: MalformedJSONError,
    IndexNotFoundError.code: IndexNotFoundError,
    IndexUnavailableError.code: IndexUnavailableError,
    IndexStaleError.code: IndexStaleError,
    SnapshotMismatchError.code: SnapshotMismatchError,
    ConflictError.code: ConflictError,
    UnsupportedError.code: UnsupportedError,
    InternalServiceError.code: InternalServiceError,
}


def service_error_from_code(
    code: str,
    message: str,
    *,
    status_code: Optional[int] = None,
    response_body: Optional[str] = None,
) -> TreeDBServiceError:
    """Build the narrowest exception class for a TreeDB service error code."""

    cls = ERROR_CLASS_BY_CODE.get(code, TreeDBServiceError)
    return cls(code, message, status_code=status_code, response_body=response_body)

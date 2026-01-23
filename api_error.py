class SteamError(Exception):
    """Base class for all Steam-related errors"""

    retryable = False
    error_type = "STEAM_ERROR"

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


# steam will not reject, reject from client side
# so ingestion RUN doesn't alloww
class SteamValidationError(SteamError):
    """Raised when client-side input is invalid
    (e.g. wrong appId, filter, language, etc.)
    """

    retryable = False
    error_type = "HTTP"


class SteamHTTPError(SteamError):
    """Raised when HTTP layer fails
    (non-200 status, timeout, connection issues)
    """

    retryable = True
    error_type = "HTTP"


class SteamAPIError(SteamError):
    """Raised when Steam API returns a logical error
    (success != 1, rate limit, internal API error)
    """

    retryable = True
    error_type = "STEAM_API"

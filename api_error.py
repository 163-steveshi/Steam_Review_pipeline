class SteamError(Exception):
    """Base class for all Steam-related errors"""

    errorType = "STEAM_ERROR"

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


# steam will not reject, reject from client side
# so ingestion RUN doesn't alloww
class SteamValidationError(SteamError):
    """Raised when client-side input is invalid
    (e.g. wrong appId, filter, language, etc.)
    """

    errorType = "SteamValidation"


class SteamHTTPError(SteamError):
    """Raised when HTTP layer fails
    (non-200 status, timeout, connection issues)
    """

    errorType = "HTTP"

    def __init__(self, message: str, statusCode: int):
        self.statusCode = statusCode
        super().__init__(message)


class SteamAPIError(SteamError):
    """Raised when Steam API returns a logical error
    (success != 1, rate limit, internal API error)
    """

    errorType = "STEAM_API"

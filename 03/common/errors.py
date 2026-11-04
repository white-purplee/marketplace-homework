import grpc


class DomainError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def invalid(message):
    raise DomainError(grpc.StatusCode.INVALID_ARGUMENT, message)


def uuid_value(value):
    from uuid import UUID
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        invalid("Expected a valid UUID")

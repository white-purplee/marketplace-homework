import logging
import os
import threading
import time
from collections import namedtuple

import grpc

log = logging.getLogger(__name__)
TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class LocalRpcError(grpc.RpcError):
    def __init__(self, code, message):
        self._code, self._message = code, message

    def code(self):
        return self._code

    def details(self):
        return self._message


class CircuitBreaker:
    def __init__(self, threshold=5, open_seconds=15, clock=time.monotonic):
        if threshold < 1 or open_seconds <= 0:
            raise ValueError("Invalid circuit breaker settings")
        self.threshold, self.open_seconds, self.clock = threshold, open_seconds, clock
        self.state, self.failures, self.opened_at, self.epoch = "CLOSED", 0, 0, 0
        self.lock = threading.Lock()

    def _transition(self, state):
        log.info("circuit_transition from=%s to=%s", self.state, state)
        self.state = state
        self.epoch += 1

    def enter(self):
        with self.lock:
            if self.state == "OPEN" and self.clock() - self.opened_at >= self.open_seconds:
                self._transition("HALF_OPEN")
                return self.epoch, True
            if self.state != "CLOSED":
                raise LocalRpcError(grpc.StatusCode.UNAVAILABLE, "Flight service circuit is open; retry later")
            return self.epoch, False

    def finish(self, token, code):
        with self.lock:
            epoch, probe = token
            if epoch != self.epoch:
                return  # A late result must not close a newer open circuit.
            if code in TRANSIENT:
                self.failures += 1
                if probe or self.failures >= self.threshold:
                    self.opened_at = self.clock()
                    self._transition("OPEN")
            else:
                self.failures = 0
                if probe:
                    self._transition("CLOSED")


class CallDetails(namedtuple("CallDetails", "method timeout metadata credentials wait_for_ready compression"), grpc.ClientCallDetails):
    pass


class ResilienceInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, key, breaker=None, attempts=3, attempt_timeout=1, total_timeout=4,
                 initial_delay=0.1, clock=time.monotonic, sleep=time.sleep):
        if not key or not 1 <= attempts <= 3 or min(attempt_timeout, total_timeout, initial_delay) <= 0:
            raise ValueError("Invalid gRPC client configuration")
        self.key, self.breaker = key, breaker or CircuitBreaker()
        self.attempts, self.attempt_timeout, self.total_timeout = attempts, attempt_timeout, total_timeout
        self.initial_delay, self.clock, self.sleep = initial_delay, clock, sleep

    def intercept_unary_unary(self, continuation, details, request):
        token = self.breaker.enter()
        deadline = self.clock() + min(details.timeout if details.timeout is not None else self.total_timeout, self.total_timeout)
        code = grpc.StatusCode.UNKNOWN
        try:
            for attempt in range(1 if token[1] else self.attempts):
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise LocalRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "Flight call deadline exceeded")
                metadata = [(k, v) for k, v in (details.metadata or []) if k != "x-api-key"]
                metadata.append(("x-api-key", self.key))
                call_details = CallDetails(
                    details.method, min(remaining, self.attempt_timeout), metadata,
                    details.credentials, False, getattr(details, "compression", None),
                )
                call = continuation(call_details, request)
                code = call.code()
                log.info("grpc_attempt method=%s attempt=%d code=%s", details.method, attempt + 1, code.name)
                if code not in TRANSIENT or attempt + 1 == (1 if token[1] else self.attempts):
                    return call
                delay = self.initial_delay * 2 ** attempt
                if deadline - self.clock() <= delay:
                    raise LocalRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "Flight retry budget exhausted")
                self.sleep(delay)
        except grpc.RpcError as exc:
            code = exc.code()
            raise
        finally:
            self.breaker.finish(token, code)


def configured_interceptor():
    return ResilienceInterceptor(
        os.environ["FLIGHT_API_KEY"],
        CircuitBreaker(int(os.getenv("CB_FAILURE_THRESHOLD", "5")), float(os.getenv("CB_OPEN_SECONDS", "15"))),
        attempts=int(os.getenv("GRPC_MAX_ATTEMPTS", "3")),
        attempt_timeout=float(os.getenv("GRPC_ATTEMPT_TIMEOUT", "1")),
        total_timeout=float(os.getenv("GRPC_TOTAL_TIMEOUT", "4")),
        initial_delay=float(os.getenv("RETRY_INITIAL_DELAY", "0.1")),
    )

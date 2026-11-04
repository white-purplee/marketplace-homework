from concurrent.futures import ThreadPoolExecutor
import grpc
import pytest
from booking_service.resilience import CircuitBreaker, LocalRpcError, ResilienceInterceptor, CallDetails


class Clock:
    def __init__(self):
        self.now, self.sleeps = 0, []

    def __call__(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


class Call:
    def __init__(self, status):
        self.status = status

    def code(self):
        return self.status


DETAILS = CallDetails("/flight.v1.FlightService/GetFlight", None, [], None, False, None)


def test_retry_is_bounded_and_injects_credentials():
    clock = Clock()
    policy = ResilienceInterceptor("secret", clock=clock, sleep=clock.sleep)
    seen = []
    def continuation(details, request):
        seen.append(details)
        return Call(grpc.StatusCode.UNAVAILABLE)
    result = policy.intercept_unary_unary(continuation, DETAILS, object())
    assert result.code() == grpc.StatusCode.UNAVAILABLE
    assert len(seen) == 3
    assert clock.sleeps == [0.1, 0.2]
    assert all(dict(item.metadata)["x-api-key"] == "secret" and item.timeout <= 1 for item in seen)
    assert policy.breaker.failures == 1  # Logical call, not each attempt.


@pytest.mark.parametrize("status", [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND,
                                   grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.UNAUTHENTICATED])
def test_business_errors_are_not_retried(status):
    seen = []
    policy = ResilienceInterceptor("secret", sleep=lambda _: pytest.fail("Unexpected retry"))
    policy.intercept_unary_unary(lambda *_: seen.append(1) or Call(status), DETAILS, object())
    assert len(seen) == 1
    assert policy.breaker.failures == 0


def test_circuit_open_single_probe_and_recovery():
    clock = Clock()
    breaker = CircuitBreaker(threshold=2, open_seconds=5, clock=clock)
    for _ in range(2):
        token = breaker.enter()
        breaker.finish(token, grpc.StatusCode.UNAVAILABLE)
    assert breaker.state == "OPEN"
    with pytest.raises(LocalRpcError):
        breaker.enter()
    clock.now = 6
    def enter():
        try:
            return breaker.enter()
        except LocalRpcError:
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        tokens = list(pool.map(lambda _: enter(), range(16)))
    probes = [t for t in tokens if t is not None]
    assert len(probes) == 1 and probes[0][1]
    breaker.finish(probes[0], grpc.StatusCode.NOT_FOUND)
    assert breaker.state == "CLOSED"


def test_late_success_cannot_close_newly_opened_circuit():
    breaker = CircuitBreaker(threshold=1)
    late, failing = breaker.enter(), breaker.enter()
    breaker.finish(failing, grpc.StatusCode.DEADLINE_EXCEEDED)
    breaker.finish(late, grpc.StatusCode.OK)
    assert breaker.state == "OPEN"


def test_total_deadline_stops_retries():
    clock = Clock()
    policy = ResilienceInterceptor("secret", total_timeout=0.15, clock=clock, sleep=clock.sleep)
    calls = []
    with pytest.raises(LocalRpcError) as error:
        policy.intercept_unary_unary(lambda *_: calls.append(1) or Call(grpc.StatusCode.DEADLINE_EXCEEDED), DETAILS, None)
    assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    assert len(calls) == 2


def test_half_open_failure_reopens_and_does_not_retry():
    clock = Clock()
    breaker = CircuitBreaker(1, 1, clock)
    breaker.finish(breaker.enter(), grpc.StatusCode.UNAVAILABLE)
    clock.now = 2
    calls = []
    policy = ResilienceInterceptor("secret", breaker=breaker, clock=clock, sleep=clock.sleep)
    policy.intercept_unary_unary(lambda *_: calls.append(1) or Call(grpc.StatusCode.UNAVAILABLE), DETAILS, None)
    assert len(calls) == 1 and breaker.state == "OPEN"


def test_open_circuit_makes_no_network_call():
    breaker = CircuitBreaker(1)
    breaker.finish(breaker.enter(), grpc.StatusCode.UNAVAILABLE)
    with pytest.raises(LocalRpcError):
        ResilienceInterceptor("secret", breaker).intercept_unary_unary(
            lambda *_: pytest.fail("Circuit must not call transport"), DETAILS, None)

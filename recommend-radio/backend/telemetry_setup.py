from urllib.parse import urlsplit, urlunsplit

from agent_memory_runtime.telemetry import configure


def _safe_url(url):
    parsed = urlsplit(url or "")
    host = parsed.hostname or ""
    if parsed.port:
        host += ":" + str(parsed.port)
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _client_hook(span, request):
    if span and span.is_recording():
        clean = _safe_url(request.url)
        span.set_attribute("http.url", clean)
        span.set_attribute("url.full", clean)
        span.set_attribute("url.query", "")


def _server_hook(span, environ):
    if span and span.is_recording():
        path = environ.get("PATH_INFO", "/")
        span.set_attribute("http.target", path)
        span.set_attribute("url.query", "")
        scheme = environ.get("wsgi.url_scheme", "http")
        host = environ.get("HTTP_HOST", "localhost")
        span.set_attribute("http.url", scheme + "://" + host + path)
        span.set_attribute("url.full", scheme + "://" + host + path)


def setup(service: str, app=None):
    configure(service)
    from agent_memory_runtime.agent.sampling import register_sampling_observer
    from full_trace import record_sampling
    register_sampling_observer(record_sampling)
    from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient, GrpcInstrumentorServer
    from opentelemetry.instrumentation.requests import RequestsInstrumentor

    for instrumentor in (GrpcInstrumentorClient(), GrpcInstrumentorServer()):
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument()
    if not RequestsInstrumentor().is_instrumented_by_opentelemetry:
        RequestsInstrumentor().instrument(request_hook=_client_hook)
    if app is not None:
        from opentelemetry.instrumentation.flask import FlaskInstrumentor

        FlaskInstrumentor().instrument_app(
            app, excluded_urls="/health/live,/health/ready", request_hook=_server_hook
        )

from __future__ import annotations

import importlib.util
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests


@pytest.fixture
def proxy(monkeypatch):
    spec = importlib.util.spec_from_file_location('docker_proxy_fixture', Path(__file__).parents[1]/'docker_static_proxy.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_GET(self):
            if self.path.endswith('/cover'):
                self.send_response(303); self.send_header('Location','http://127.0.0.1:1/signed-object?signature=fixture');self.end_headers();return
            self.send_response(200)
            if self.path.startswith('/api/agent/events'):
                self.send_header('Content-Type','text/event-stream');body=b'event: done\ndata: {}\n\n'
            elif self.path.endswith('.m4s'):
                self.send_header('Content-Type','video/mp4');body=b'media-fragment'*4000
            else:
                self.send_header('Content-Type','application/json');body=b'{"ok":true}'
            self.end_headers();self.wfile.write(body);self.wfile.flush()
    upstream=ThreadingHTTPServer(('127.0.0.1',0),Upstream)
    monkeypatch.setattr(module,'BACKEND_URL',f'http://127.0.0.1:{upstream.server_port}')
    monkeypatch.setattr(module,'SSE_GATEWAY_URL',module.BACKEND_URL)
    server=ThreadingHTTPServer(('127.0.0.1',0),module.StaticProxyHandler)
    for value in (upstream,server):threading.Thread(target=value.serve_forever,daemon=True).start()
    yield f'http://127.0.0.1:{server.server_port}'
    server.shutdown();upstream.shutdown();server.server_close();upstream.server_close()


def test_signed_redirect_remains_for_browser_instead_of_proxy_fetch(proxy):
    response=requests.get(proxy+'/api/content/c/cover',allow_redirects=False,timeout=3)
    assert response.status_code==303
    assert response.headers['Location'].startswith('http://127.0.0.1:1/signed-object')


def test_sse_eof_closes_http11_response_cleanly(proxy):
    response=requests.get(proxy+'/api/agent/events',timeout=3)
    assert response.text=='event: done\ndata: {}\n\n'
    assert response.headers['Connection']=='close'


def test_response_framing_for_json_and_cold_media(proxy):
    response=requests.get(proxy+'/api/example',timeout=3)
    assert response.json()=={'ok':True} and int(response.headers['Content-Length'])==len(response.content)
    fragment=requests.get(proxy+'/api/media/streams/p/seg_000000.m4s',timeout=3)
    assert fragment.content==b'media-fragment'*4000

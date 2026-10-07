from __future__ import annotations

import os
from typing import Any

from amem_bridge import AmemBridge
from amem_grpc_bridge import AmemGrpcBridge, GrpcProfileProjector
from profile_projector import ProfileProjector
from rabbitmq_bus import wrap_behavior_bridge


def build_amem_runtime() -> tuple[Any, Any]:
    transport = os.getenv("AMEM_TRANSPORT", "embedded").strip().lower()
    if transport == "grpc":
        bridge = AmemGrpcBridge.from_env()
        return wrap_behavior_bridge(bridge), GrpcProfileProjector(bridge)
    bridge = AmemBridge.from_env()
    projector = ProfileProjector(bridge)
    bind_invalidator = getattr(bridge, "bind_dream_cache_invalidator", None)
    if callable(bind_invalidator):
        bind_invalidator(projector.clear_cache)
    return wrap_behavior_bridge(bridge), projector

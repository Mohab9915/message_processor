"""
Health Check Router for Message Processor Service
"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any, Optional
import time
import asyncio

from shared.config.settings import get_settings
from shared.utils.logger import get_service_logger

settings = get_settings("message-processor")
logger = get_service_logger("health")

router = APIRouter()


@router.get("/")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "service": "message-processor",
        "timestamp": time.time(),
        "version": "1.0.0"
    }


@router.get("/detailed")
async def detailed_health_check():
    """Detailed health check with component status"""

    # This will be populated by the main application
    from ..main import database_manager, ai_orchestrator, message_queue

    health_status = {
        "service": "message-processor",
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0",
        "checks": {},
        "dependencies": {}
    }

    # Check database
    if database_manager and database_manager.is_connected():
        try:
            pool_status = database_manager.get_pool_status()
            health_status["dependencies"]["database"] = {
                "status": "healthy",
                "connection_pool": pool_status,
                "last_check": time.time()
            }
        except Exception as e:
            health_status["dependencies"]["database"] = {
                "status": "degraded",
                "error": str(e),
                "last_check": time.time()
            }
            health_status["status"] = "degraded"
    else:
        health_status["dependencies"]["database"] = {
            "status": "unhealthy",
            "error": "Not connected",
            "last_check": time.time()
        }
        health_status["status"] = "unhealthy"

    # Check AI orchestrator
    if ai_orchestrator and ai_orchestrator.is_ready():
        health_status["dependencies"]["ai_orchestrator"] = {
            "status": "healthy",
            "available_providers": ai_orchestrator.get_available_providers(),
            "provider_status": ai_orchestrator.get_provider_status(),
            "last_check": time.time()
        }
    else:
        health_status["dependencies"]["ai_orchestrator"] = {
            "status": "unhealthy",
            "error": "Not ready",
            "last_check": time.time()
        }
        health_status["status"] = "degraded"

    # Check message queue
    if message_queue and message_queue.is_active():
        try:
            queue_health = await message_queue.get_queue_health()
            health_status["dependencies"]["message_queue"] = {
                "status": queue_health["status"],
                "queue_stats": queue_health.get("queue_stats", {}),
                "last_check": time.time()
            }

            if queue_health["status"] in ["critical", "error"]:
                health_status["status"] = "warning"

        except Exception as e:
            health_status["dependencies"]["message_queue"] = {
                "status": "unhealthy",
                "error": str(e),
                "last_check": time.time()
            }
            health_status["status"] = "degraded"
    else:
        health_status["dependencies"]["message_queue"] = {
            "status": "unhealthy",
            "error": "Not active",
            "last_check": time.time()
        }
        health_status["status"] = "degraded"

    return health_status


@router.get("/ready")
async def readiness_check():
    """Readiness check - indicates if service is ready to receive traffic"""

    # Check critical dependencies
    critical_checks = []

    # Check database
    from ..main import database_manager
    db_ready = database_manager and database_manager.is_connected()
    critical_checks.append(db_ready)

    # Check message queue
    from ..main import message_queue
    queue_ready = message_queue and message_queue.is_active()
    critical_checks.append(queue_ready)

    # Service is ready if all critical checks pass
    all_healthy = all(critical_checks)

    if all_healthy:
        return {
            "status": "ready",
            "timestamp": time.time(),
            "checks": {
                "database": db_ready,
                "message_queue": queue_ready
            }
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is not ready"
        )


@router.get("/live")
async def liveness_check():
    """Liveness check - indicates if service is alive"""

    return {
        "status": "alive",
        "timestamp": time.time(),
        "uptime": time.time()  # Would be actual uptime in real implementation
    }


@router.get("/version")
async def version_info():
    """Get version information"""
    return {
        "service": "message-processor",
        "version": "1.0.0",
        "build_info": {
            "timestamp": "2025-11-15T23:00:00Z",
            "git_commit": "def456abc",  # Would be actual commit hash
            "environment": settings.environment
        },
        "dependencies": {
            "fastapi": "0.104.1",
            "python": "3.11",
            "sqlalchemy": "2.0.23",
            "redis": "5.0.1"
        }
    }


@router.get("/metrics")
async def health_metrics():
    """Health-related metrics"""

    # This would pull actual metrics from the services
    return {
        "uptime_seconds": time.time(),  # Would be actual uptime
        "last_health_check": time.time(),
        "total_checks": 0,  # Would track actual check count
        "healthy_checks": 0,
        "unhealthy_checks": 0,
        "component_status": {
            "database": "unknown",  # Would be actual status
            "ai_orchestrator": "unknown",
            "message_queue": "unknown"
        }
    }
"""
Message Processor Service - Core Message Processing Service
Handles incoming messages, orchestrates AI processing, and manages conversations
"""
from fastapi import FastAPI, Request, HTTPException, status, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import asyncio
import time
import json
from typing import Dict, Any, List, Optional
import httpx

from shared.config.settings import get_settings
from shared.utils.logger import get_service_logger, log_requests, MessageLogger
from shared.models.database import Conversation, Message, MessageIntent, ConversationContext
from .routers import messages, conversations, webhooks, health, ai_processing
from .database import DatabaseManager
from .ai_orchestrator import AIOrchestrator
from .message_queue import MessageQueue
from .exceptions import MessageProcessingException, setup_exception_handlers

# Initialize settings and logger
settings = get_settings("message-processor")
logger = get_service_logger("message-processor")
message_logger = MessageLogger(logger)

# Global services
database_manager: Optional[DatabaseManager] = None
ai_orchestrator: Optional[AIOrchestrator] = None
message_queue: Optional[MessageQueue] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    global database_manager, ai_orchestrator, message_queue

    logger.info("message_processor_startup", version="1.0.0")

    try:
        # Initialize database connection
        database_manager = DatabaseManager(settings.database_url)
        await database_manager.initialize()
        logger.info("database_initialized")

        # Initialize AI orchestrator
        ai_orchestrator = AIOrchestrator(settings)
        await ai_orchestrator.initialize()
        logger.info("ai_orchestrator_initialized")

        # Initialize message queue
        message_queue = MessageQueue(settings)
        await message_queue.initialize()
        logger.info("message_queue_initialized")

        # Start background processors
        background_tasks = [
            asyncio.create_task(message_processor_worker()),
            asyncio.create_task(ai_processing_worker()),
            asyncio.create_task(context_cleanup_worker()),
        ]

        logger.info("background_workers_started", workers_count=len(background_tasks))

        try:
            yield
        finally:
            # Cleanup background tasks
            for task in background_tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Cleanup services
            if message_queue:
                await message_queue.cleanup()
            if ai_orchestrator:
                await ai_orchestrator.cleanup()
            if database_manager:
                await database_manager.cleanup()

            logger.info("message_processor_shutdown")

    except Exception as e:
        logger.error(
            "message_processor_startup_error",
            error=str(e),
            exc_info=True
        )
        raise


# Initialize FastAPI app
app = FastAPI(
    title="Message Processor Service",
    description="Core message processing and AI orchestration service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request logging middleware
app.middleware("http")(log_requests)

# Exception handlers
setup_exception_handlers(app)

# Include routers
app.include_router(
    health.router,
    prefix="/health",
    tags=["Health"]
)

app.include_router(
    webhooks.router,
    prefix="/api/v1/webhooks",
    tags=["Webhooks"]
)

app.include_router(
    conversations.router,
    prefix="/api/v1/conversations",
    tags=["Conversations"]
)

app.include_router(
    messages.router,
    prefix="/api/v1/messages",
    tags=["Messages"]
)

app.include_router(
    ai_processing.router,
    prefix="/api/v1/ai",
    tags=["AI Processing"]
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "message-processor",
        "version": "1.0.0",
        "status": "running",
        "timestamp": time.time(),
    }


@app.get("/api/v1/status")
async def service_status():
    """Get comprehensive service status"""

    status_data = {
        "service": {
            "name": "message-processor",
            "version": "1.0.0",
            "status": "healthy",
            "environment": settings.environment,
            "timestamp": time.time(),
        },
        "components": {
            "database": {
                "status": "connected" if database_manager and database_manager.is_connected() else "disconnected",
            },
            "ai_orchestrator": {
                "status": "ready" if ai_orchestrator and ai_orchestrator.is_ready() else "not_ready",
                "available_providers": ai_orchestrator.get_available_providers() if ai_orchestrator else [],
            },
            "message_queue": {
                "status": "active" if message_queue and message_queue.is_active() else "inactive",
                "queue_size": await message_queue.get_queue_size() if message_queue else 0,
            }
        }
    }

    return status_data


# Background workers
async def message_processor_worker():
    """Background worker for processing messages"""
    logger.info("message_processor_worker_started")

    while True:
        try:
            # Get message from queue
            message_data = await message_queue.get_message("processing", timeout=5.0)

            if message_data:
                await process_message_background(message_data)
            else:
                await asyncio.sleep(0.1)  # Brief pause if no messages

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(
                "message_processor_worker_error",
                error=str(e),
                exc_info=True
            )
            await asyncio.sleep(5)  # Wait before retrying


async def ai_processing_worker():
    """Background worker for AI processing"""
    logger.info("ai_processing_worker_started")

    while True:
        try:
            # Get AI task from queue
            ai_task = await message_queue.get_message("ai_processing", timeout=5.0)

            if ai_task:
                await process_ai_task_background(ai_task)
            else:
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(
                "ai_processing_worker_error",
                error=str(e),
                exc_info=True
            )
            await asyncio.sleep(5)


async def context_cleanup_worker():
    """Background worker for cleaning up expired context"""
    logger.info("context_cleanup_worker_started")

    while True:
        try:
            if database_manager:
                # Clean expired context (run every hour)
                await database_manager.cleanup_expired_context()
                await asyncio.sleep(3600)  # 1 hour
            else:
                await asyncio.sleep(60)  # Wait and retry

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(
                "context_cleanup_worker_error",
                error=str(e),
                exc_info=True
            )
            await asyncio.sleep(300)  # 5 minutes


# Background processing functions
async def process_message_background(message_data: Dict[str, Any]):
    """Process message in background"""
    start_time = time.time()
    message_id = message_data.get("message_id")

    try:
        message_logger.log_message_received(
            message_id=message_id,
            platform=message_data.get("platform"),
            conversation_id=message_data.get("conversation_id"),
            content=message_data.get("content", "")
        )

        # Stage 1: Extract entities and basic info
        processed_data = await preprocess_message(message_data)

        # Stage 2: Queue for AI processing
        ai_task = {
            "task_id": f"ai_{message_id}",
            "task_type": "message_processing",
            "message_data": processed_data,
            "created_at": time.time(),
        }

        await message_queue.add_message("ai_processing", ai_task)

        # Update message status
        await database_manager.update_message_status(
            message_id=message_id,
            status="queued_for_ai",
            processing_metadata=processed_data
        )

        processing_time = time.time() - start_time

        message_logger.log_message_processed(
            message_id=message_id,
            processing_time=processing_time,
            intent=processed_data.get("detected_intent"),
            entities=processed_data.get("entities", {})
        )

        logger.info(
            "message_processed_successfully",
            message_id=message_id,
            processing_time=processing_time
        )

    except Exception as e:
        processing_time = time.time() - start_time
        message_logger.log_message_error(
            message_id=message_id,
            error=str(e),
            error_type=type(e).__name__
        )

        logger.error(
            "message_processing_error",
            message_id=message_id,
            error=str(e),
            processing_time=processing_time,
            exc_info=True
        )

        # Update message status to error
        if database_manager and message_id:
            await database_manager.update_message_status(
                message_id=message_id,
                status="error",
                error_message=str(e)
            )


async def process_ai_task_background(ai_task: Dict[str, Any]):
    """Process AI task in background"""
    start_time = time.time()
    task_id = ai_task.get("task_id")

    try:
        task_type = ai_task.get("task_type")
        message_data = ai_task.get("message_data")

        if task_type == "message_processing":
            await process_message_ai(message_data)
        elif task_type == "conversation_context":
            await update_conversation_context(message_data)
        else:
            logger.warning(
                "unknown_ai_task_type",
                task_type=task_type,
                task_id=task_id
            )

        processing_time = time.time() - start_time

        logger.info(
            "ai_task_completed",
            task_id=task_id,
            task_type=task_type,
            processing_time=processing_time
        )

    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(
            "ai_task_error",
            task_id=task_id,
            error=str(e),
            processing_time=processing_time,
            exc_info=True
        )


async def preprocess_message(message_data: Dict[str, Any]) -> Dict[str, Any]:
    """Preprocess message to extract basic information"""

    processed_data = {
        "message_id": message_data.get("message_id"),
        "conversation_id": message_data.get("conversation_id"),
        "platform": message_data.get("platform"),
        "content": message_data.get("content", ""),
        "sender_info": message_data.get("sender_info", {}),
        "message_type": message_data.get("message_type", "text"),
        "direction": message_data.get("direction", "inbound"),
        "timestamp": message_data.get("timestamp", time.time()),
    }

    # Extract basic entities (can be enhanced with NLP)
    content = processed_data["content"].lower()

    # Simple keyword-based intent detection (will be enhanced by AI)
    detected_intent = "greeting"
    entities = {}

    if any(word in content for word in ["price", "cost", "كم", "כמה"]):
        detected_intent = "price_inquiry"
    elif any(word in content for word in ["order", "status", "طلب", "הזמנה"]):
        detected_intent = "order_status"
    elif any(word in content for word in ["hello", "hi", "مرحبا", "שלום"]):
        detected_intent = "greeting"
    elif any(word in content for word in ["problem", "issue", "مشكلة", "בעיה"]):
        detected_intent = "complaint"

    # Extract potential product references
    if "size" in content:
        entities["size_mentioned"] = True
    if "color" in content:
        entities["color_mentioned"] = True
    if any(char.isdigit() for char in content):
        entities["numbers_mentioned"] = True

    processed_data.update({
        "detected_intent": detected_intent,
        "entities": entities,
        "language": detect_language(content),
        "preprocessing_timestamp": time.time(),
    })

    return processed_data


def detect_language(text: str) -> str:
    """Simple language detection (will be enhanced by AI)"""

    # Check for Arabic characters
    if any('\u0600' <= char <= '\u06FF' for char in text):
        return "arabic"

    # Check for Hebrew characters
    if any('\u0590' <= char <= '\u05FF' for char in text):
        return "hebrew"

    # Default to English
    return "english"


async def process_message_ai(message_data: Dict[str, Any]):
    """Process message with AI"""

    if not ai_orchestrator:
        logger.warning("ai_orchestrator_not_available", message_id=message_data.get("message_id"))
        return

    try:
        # Get conversation context
        conversation_context = await database_manager.get_conversation_context(
            message_data.get("conversation_id")
        )

        # Process with AI
        ai_result = await ai_orchestrator.process_message(
            message=message_data.get("content", ""),
            context=conversation_context,
            platform=message_data.get("platform"),
            language=message_data.get("language")
        )

        # Save AI result
        await database_manager.save_ai_result(
            message_id=message_data.get("message_id"),
            ai_result=ai_result
        )

        # Generate response if needed
        if message_data.get("direction") == "inbound" and ai_result.get("response"):
            await generate_and_send_response(message_data, ai_result)

        logger.info(
            "message_ai_processed",
            message_id=message_data.get("message_id"),
            intent=ai_result.get("intent"),
            confidence=ai_result.get("confidence")
        )

    except Exception as e:
        logger.error(
            "message_ai_processing_error",
            message_id=message_data.get("message_id"),
            error=str(e),
            exc_info=True
        )


async def generate_and_send_response(message_data: Dict[str, Any], ai_result: Dict[str, Any]):
    """Generate and send response message"""

    try:
        response_data = {
            "conversation_id": message_data.get("conversation_id"),
            "content": ai_result.get("response"),
            "direction": "outbound",
            "message_type": "text",
            "platform": message_data.get("platform"),
            "sender_info": {"sender_type": "ai_bot"},
            "in_reply_to": message_data.get("message_id"),
            "ai_metadata": {
                "provider": ai_result.get("provider"),
                "model": ai_result.get("model"),
                "intent": ai_result.get("intent"),
                "confidence": ai_result.get("confidence"),
            }
        }

        # Save response to database
        response_message = await database_manager.create_message(response_data)

        # Queue for sending to platform
        send_task = {
            "task_id": f"send_{response_message.id}",
            "task_type": "send_message",
            "message_data": response_data,
            "platform": message_data.get("platform"),
            "created_at": time.time(),
        }

        await message_queue.add_message("sending", send_task)

        logger.info(
            "response_generated",
            original_message_id=message_data.get("message_id"),
            response_message_id=response_message.id,
            platform=message_data.get("platform")
        )

    except Exception as e:
        logger.error(
            "response_generation_error",
            message_id=message_data.get("message_id"),
            error=str(e),
            exc_info=True
        )


# Service health check
@app.get("/api/v1/components/health")
async def components_health():
    """Detailed health check of all components"""

    health_status = {
        "database": {
            "status": "healthy" if database_manager and database_manager.is_connected() else "unhealthy",
            "connection_pool": database_manager.get_pool_status() if database_manager else None,
        },
        "ai_orchestrator": {
            "status": "healthy" if ai_orchestrator and ai_orchestrator.is_ready() else "unhealthy",
            "providers": ai_orchestrator.get_provider_status() if ai_orchestrator else {},
        },
        "message_queue": {
            "status": "healthy" if message_queue and message_queue.is_active() else "unhealthy",
            "queue_sizes": await message_queue.get_all_queue_sizes() if message_queue else {},
        }
    }

    return health_status


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.service_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        access_log=True,
    )
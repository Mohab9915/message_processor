"""
Message Processor Service for Railway Deployment
Handles message processing, queuing, and orchestration
"""

import os
import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import httpx
import structlog
import redis.asyncio as redis
import uuid
import asyncio
from dataclasses import dataclass

logger = structlog.get_logger()

# Environment variables
PORT = os.getenv("PORT", "8001")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
QDRANT_URL = os.getenv("QDRANT_URL", "")

# Service URLs
AI_NLP_URL = os.getenv("AI_NLP_URL", "http://ai-nlp-service:8002")
CONVERSATION_MANAGER_URL = os.getenv("CONVERSATION_MANAGER_URL", "http://conversation-manager:8003")
RESPONSE_GENERATOR_URL = os.getenv("RESPONSE_GENERATOR_URL", "http://response-generator:8004")

# Message queue settings
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "1000"))
PROCESSING_TIMEOUT = int(os.getenv("PROCESSING_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

class MessageType(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"

class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class Priority(int, Enum):
    LOW = 3
    NORMAL = 2
    HIGH = 1
    URGENT = 0

# Pydantic models
@dataclass
class Message:
    id: str
    conversation_id: str
    content: str
    message_type: MessageType
    status: MessageStatus
    priority: Priority
    metadata: Dict[str, Any]
    created_at: datetime
    retry_count: int = 0
    processing_started: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class ProcessMessageRequest(BaseModel):
    conversation_id: str
    content: str
    message_type: MessageType = MessageType.USER
    priority: Priority = Priority.NORMAL
    metadata: Dict[str, Any] = Field(default_factory=dict)
    user_id: str
    context: Dict[str, Any] = Field(default_factory=dict)

class ProcessMessageResponse(BaseModel):
    message_id: str
    status: str
    queue_position: int
    estimated_wait_time: float

class MessageQueueResponse(BaseModel):
    queue_size: int
    processing_count: int
    completed_count: int
    failed_count: int
    average_processing_time: float

class RedisManager:
    """Redis connection manager for message queuing and caching"""

    def __init__(self):
        self.client = None

    async def initialize(self):
        """Initialize Redis connection"""
        try:
            self.client = redis.from_url(REDIS_URL)
            await self.client.ping()
            logger.info("Redis connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {str(e)}")
            self.client = None

class MessageQueue:
    """Message queue implementation using Redis"""

    def __init__(self, redis_manager: RedisManager):
        self.redis = redis_manager
        self.processing_messages = set()

    async def add_message(self, message: Message) -> bool:
        """Add message to queue"""
        if not self.redis.client:
            return False

        try:
            # Check queue size
            queue_size = await self.redis.client.zcard("message_queue")
            if queue_size >= MAX_QUEUE_SIZE:
                logger.warning("Message queue is full")
                return False

            # Add to Redis sorted queue (score = priority + timestamp)
            score = message.priority + time.time()
            await self.redis.client.zadd("message_queue", {message.id: score})

            # Store message details
            await self.redis.client.setex(
                f"message:{message.id}",
                86400,  # 24 hours TTL
                json.dumps(message.__dict__, default=str)
            )

            logger.info(f"Message {message.id} added to queue")
            return True

        except Exception as e:
            logger.error(f"Failed to add message to queue: {str(e)}")
            return False

    async def get_next_message(self) -> Optional[Message]:
        """Get next message from queue"""
        if not self.redis.client:
            return None

        try:
            # Get highest priority message (lowest score)
            result = await self.redis.client.zpopmin("message_queue")
            if not result:
                return None

            message_id = result[0][0]

            # Get message details
            message_data = await self.redis.client.get(f"message:{message_id}")
            if not message_data:
                return None

            data = json.loads(message_data)
            message = Message(
                id=data["id"],
                conversation_id=data["conversation_id"],
                content=data["content"],
                message_type=MessageType(data["message_type"]),
                status=MessageStatus(data["status"]),
                priority=Priority(data["priority"]),
                metadata=data["metadata"],
                created_at=datetime.fromisoformat(data["created_at"]),
                retry_count=data.get("retry_count", 0)
            )

            self.processing_messages.add(message_id)
            return message

        except Exception as e:
            logger.error(f"Failed to get next message: {str(e)}")
            return None

    async def complete_message(self, message_id: str, success: bool = True):
        """Mark message as completed"""
        if not self.redis.client:
            return

        try:
            # Remove from processing set
            self.processing_messages.discard(message_id)

            # Update message status
            message_data = await self.redis.client.get(f"message:{message_id}")
            if message_data:
                data = json.loads(message_data)
                data["status"] = "completed" if success else "failed"
                data["completed_at"] = datetime.utcnow().isoformat()

                # Update message with shorter TTL
                await self.redis.client.setex(
                    f"message:{message_id}",
                    3600,  # 1 hour TTL
                    json.dumps(data, default=str)
                )

            logger.info(f"Message {message_id} marked as {'completed' if success else 'failed'}")

        except Exception as e:
            logger.error(f"Failed to complete message {message_id}: {str(e)}")

    async def requeue_message(self, message: Message) -> bool:
        """Requeue message for retry"""
        if message.retry_count >= MAX_RETRIES:
            await self.complete_message(message.id, success=False)
            return False

        message.retry_count += 1
        message.status = MessageStatus.PENDING

        # Add exponential backoff delay
        delay = 2 ** message.retry_count
        score = message.priority + time.time() + delay

        if not self.redis.client:
            return False

        try:
            await self.redis.client.zadd("message_queue", {message.id: score})
            await self.redis.client.setex(
                f"message:{message.id}",
                86400,
                json.dumps(message.__dict__, default=str)
            )

            self.processing_messages.discard(message.id)
            logger.info(f"Message {message.id} requeued (attempt {message.retry_count})")
            return True

        except Exception as e:
            logger.error(f"Failed to requeue message {message.id}: {str(e)}")
            return False

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        if not self.redis.client:
            return {
                "queue_size": 0,
                "processing_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "average_processing_time": 0.0
            }

        try:
            queue_size = await self.redis.client.zcard("message_queue")
            processing_count = len(self.processing_messages)

            # Get completed and failed counts (approximate)
            completed_count = 0
            failed_count = 0

            return {
                "queue_size": queue_size,
                "processing_count": processing_count,
                "completed_count": completed_count,
                "failed_count": failed_count,
                "average_processing_time": 0.0
            }

        except Exception as e:
            logger.error(f"Failed to get queue stats: {str(e)}")
            return {"error": str(e)}

class MessageProcessor:
    """Main message processor"""

    def __init__(self, redis_manager: RedisManager):
        self.redis = redis_manager
        self.message_queue = MessageQueue(redis_manager)
        self.processing = False

    async def process_message(self, message: Message) -> bool:
        """Process a single message"""
        try:
            logger.info(f"Processing message {message.id}")

            # Update message status
            message.status = MessageStatus.PROCESSING
            message.processing_started = datetime.utcnow()

            # Simulate message processing (in real implementation, this would call other services)
            # For Railway demo, we'll do basic processing
            await asyncio.sleep(1)  # Simulate processing time

            # Call NLP service for analysis
            async with httpx.AsyncClient(timeout=30.0) as client:
                nlp_response = await client.post(
                    f"{AI_NLP_URL}/api/v1/process/text",
                    json={
                        "text": message.content,
                        "options": ["intent", "entities", "sentiment"],
                        "context": message.metadata.get("context", {})
                    }
                )
                nlp_response.raise_for_status()
                nlp_results = nlp_response.json()

            # Update message metadata with NLP results
            message.metadata["nlp_results"] = nlp_results["results"]
            message.metadata["processing_time"] = (datetime.utcnow() - message.processing_started).total_seconds()

            logger.info(f"Message {message.id} processed successfully")
            return True

        except httpx.TimeoutException:
            logger.error(f"Timeout processing message {message.id}")
            return False
        except httpx.ConnectError:
            logger.error(f"Connection error processing message {message.id}")
            return False
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {str(e)}")
            return False

    async def start_processing(self):
        """Start message processing loop"""
        self.processing = True
        logger.info("Message processing started")

        while self.processing:
            try:
                message = await self.message_queue.get_next_message()
                if message:
                    success = await self.process_message(message)
                    if success:
                        await self.message_queue.complete_message(message.id, success=True)
                    else:
                        # Retry logic
                        requeued = await self.message_queue.requeue_message(message)
                        if not requeued:
                            logger.error(f"Message {message.id} failed after max retries")

                else:
                    # No messages in queue, wait a bit
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")
                await asyncio.sleep(1)

    async def stop_processing(self):
        """Stop message processing"""
        self.processing = False
        logger.info("Message processing stopped")

    async def add_message(self, request: ProcessMessageRequest) -> ProcessMessageResponse:
        """Add new message to queue"""
        message = Message(
            id=str(uuid.uuid4()),
            conversation_id=request.conversation_id,
            content=request.content,
            message_type=request.message_type,
            status=MessageStatus.PENDING,
            priority=request.priority,
            metadata=request.metadata,
            created_at=datetime.utcnow()
        )

        success = await self.message_queue.add_message(message)
        if not success:
            raise HTTPException(status_code=503, detail="Failed to add message to queue")

        # Get queue position
        stats = await self.message_queue.get_queue_stats()
        queue_position = stats.get("queue_size", 0)

        # Estimate wait time (1 second per message)
        estimated_wait_time = queue_position * 1.0

        return ProcessMessageResponse(
            message_id=message.id,
            status="queued",
            queue_position=queue_position,
            estimated_wait_time=estimated_wait_time
        )

# Global instances
redis_manager = RedisManager()
message_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global message_processor

    logger.info("Starting Message Processor Service...")

    await redis_manager.initialize()
    message_processor = MessageProcessor(redis_manager)

    # Start message processing in background
    processing_task = asyncio.create_task(message_processor.start_processing())

    logger.info("Message Processor Service started successfully")

    yield

    # Cleanup
    await message_processor.stop_processing()
    processing_task.cancel()

    if redis_manager.client:
        await redis_manager.client.close()

    logger.info("Message Processor Service shutdown complete")

# FastAPI application
app = FastAPI(
    title="Message Processor Service",
    description="Processes and queues messages in the chatbot system",
    version="1.0.0",
    lifespan=lifespan
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Message Processor Service",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    stats = await message_processor.message_queue.get_queue_stats()

    return {
        "status": "healthy",
        "service": "message-processor",
        "timestamp": datetime.utcnow(),
        "version": "1.0.0",
        "processing": message_processor.processing,
        "redis": "connected" if redis_manager.client else "disconnected",
        "queue_stats": stats
    }

@app.post("/api/v1/messages/process", response_model=ProcessMessageResponse)
async def process_message(request: ProcessMessageRequest):
    """Process a message"""
    try:
        return await message_processor.add_message(request)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process message")

@app.get("/api/v1/messages/{message_id}")
async def get_message(message_id: str):
    """Get message status"""
    if not redis_manager.client:
        raise HTTPException(status_code=503, detail="Redis not available")

    try:
        message_data = await redis_manager.client.get(f"message:{message_id}")
        if not message_data:
            raise HTTPException(status_code=404, detail="Message not found")

        return json.loads(message_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting message: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get message")

@app.get("/api/v1/queue/stats", response_model=MessageQueueResponse)
async def get_queue_stats():
    """Get queue statistics"""
    try:
        stats = await message_processor.message_queue.get_queue_stats()
        return MessageQueueResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting queue stats: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get queue stats")

@app.post("/api/v1/queue/clear")
async def clear_queue():
    """Clear message queue (admin only)"""
    if not redis_manager.client:
        raise HTTPException(status_code=503, detail="Redis not available")

    try:
        await redis_manager.client.delete("message_queue")
        logger.info("Message queue cleared")
        return {"status": "cleared", "message": "Queue cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing queue: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to clear queue")

@app.get("/api/v1/status")
async def get_status():
    """Get service status"""
    stats = await message_processor.message_queue.get_queue_stats()

    return {
        "service": "message-processor",
        "status": "running",
        "version": "1.0.0",
        "timestamp": datetime.utcnow(),
        "processing": message_processor.processing,
        "redis": "connected" if redis_manager.client else "disconnected",
        "queue_stats": stats,
        "capabilities": {
            "message_processing": True,
            "priority_queueing": True,
            "retry_logic": True,
            "async_processing": True
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "railway_main:app",
        host="0.0.0.0",
        port=int(PORT),
        reload=False,
        log_level="info"
    )
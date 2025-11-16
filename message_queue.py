"""
Message Queue Manager
Handles message queuing and background processing using Redis
"""
import json
import time
import asyncio
from typing import Dict, Any, List, Optional, Union
import redis.asyncio as redis
from dataclasses import dataclass, asdict

from shared.config.settings import get_settings
from shared.utils.logger import get_service_logger

settings = get_settings("message-processor")
logger = get_service_logger("message_queue")


@dataclass
class QueueMessage:
    """Message structure for queue"""
    task_id: str
    task_type: str
    data: Dict[str, Any]
    created_at: float
    priority: int = 1
    retry_count: int = 0
    max_retries: int = 3
    error: Optional[str] = None
    status: str = "pending"  # pending, processing, completed, failed

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueueMessage':
        """Create from dictionary"""
        return cls(**data)


class MessageQueue:
    """Redis-based message queue manager"""

    def __init__(self, settings):
        self.settings = settings
        self.redis_client: Optional[redis.Redis] = None
        self.queue_names = {
            "processing": "chatbot:processing_queue",
            "ai_processing": "chatbot:ai_processing_queue",
            "sending": "chatbot:sending_queue",
            "failed": "chatbot:failed_queue",
            "completed": "chatbot:completed_queue"
        }
        self.processing_set = "chatbot:processing_set"  # Track currently processing tasks

    async def initialize(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )

            # Test connection
            await self.redis_client.ping()
            logger.info("message_queue_redis_connected", url=self.settings.redis_url)

        except Exception as e:
            logger.error(
                "message_queue_redis_connection_failed",
                error=str(e),
                url=self.settings.redis_url
            )
            raise

    async def cleanup(self):
        """Cleanup Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("message_queue_redis_closed")

    def is_active(self) -> bool:
        """Check if queue is active"""
        return self.redis_client is not None

    async def add_message(
        self,
        queue_name: str,
        message: Union[QueueMessage, Dict[str, Any]],
        priority: int = 1
    ) -> str:
        """Add message to queue"""

        if not self.redis_client:
            raise Exception("Redis client not initialized")

        if isinstance(message, dict):
            # Create QueueMessage from dict
            queue_message = QueueMessage(
                task_id=message.get("task_id", f"task_{int(time.time() * 1000000)}"),
                task_type=message.get("task_type", "unknown"),
                data=message.get("data", message),
                created_at=message.get("created_at", time.time()),
                priority=message.get("priority", priority),
                retry_count=message.get("retry_count", 0),
                max_retries=message.get("max_retries", 3),
                error=message.get("error"),
                status=message.get("status", "pending")
            )
        else:
            queue_message = message

        # Add priority timestamp for priority queue
        priority_score = self._calculate_priority_score(queue_message.priority, queue_message.created_at)

        queue_key = self.queue_names.get(queue_name)
        if not queue_key:
            raise ValueError(f"Unknown queue: {queue_name}")

        try:
            # Add to Redis sorted set (priority queue)
            await self.redis_client.zadd(
                queue_key,
                {json.dumps(queue_message.to_dict()): priority_score}
            )

            logger.info(
                "message_queued",
                task_id=queue_message.task_id,
                queue=queue_name,
                priority=queue_message.priority
            )

            return queue_message.task_id

        except Exception as e:
            logger.error(
                "message_queue_error",
                task_id=queue_message.task_id,
                queue=queue_name,
                error=str(e)
            )
            raise

    async def get_message(
        self,
        queue_name: str,
        timeout: float = 5.0
    ) -> Optional[QueueMessage]:
        """Get next message from queue with blocking"""

        if not self.redis_client:
            return None

        queue_key = self.queue_names.get(queue_name)
        if not queue_key:
            raise ValueError(f"Unknown queue: {queue_name}")

        try:
            # Get highest priority (lowest score) message
            result = await self.redis_client.bzpopmin(queue_key, timeout=int(timeout))

            if result:
                _, message_data, score = result
                message_dict = json.loads(message_data)
                message = QueueMessage.from_dict(message_dict)

                # Add to processing set
                await self.redis_client.sadd(self.processing_set, message.task_id)
                message.status = "processing"

                logger.info(
                    "message_dequeued",
                    task_id=message.task_id,
                    queue=queue_name,
                    priority_score=score
                )

                return message

            return None

        except Exception as e:
            logger.error(
                "get_message_error",
                queue=queue_name,
                error=str(e)
            )
            return None

    async def complete_message(
        self,
        task_id: str,
        result: Dict[str, Any] = None
    ):
        """Mark message as completed"""

        if not self.redis_client:
            return

        try:
            # Remove from processing set
            await self.redis_client.srem(self.processing_set, task_id)

            # Add to completed queue for analytics
            completed_message = {
                "task_id": task_id,
                "completed_at": time.time(),
                "result": result
            }

            await self.redis_client.lpush(
                self.queue_names["completed"],
                json.dumps(completed_message)
            )

            # Keep only last 1000 completed messages
            await self.redis_client.ltrim(self.queue_names["completed"], 0, 999)

            logger.info(
                "message_completed",
                task_id=task_id
            )

        except Exception as e:
            logger.error(
                "complete_message_error",
                task_id=task_id,
                error=str(e)
            )

    async def fail_message(
        self,
        task_id: str,
        error: str,
        retry: bool = True
    ):
        """Mark message as failed"""

        if not self.redis_client:
            return

        try:
            # Remove from processing set
            await self.redis_client.srem(self.processing_set, task_id)

            # Get original message (this is a simplified approach)
            # In production, you'd want to track the full message
            failed_message = {
                "task_id": task_id,
                "error": error,
                "failed_at": time.time(),
                "retry": retry
            }

            if retry:
                # Re-queue with increased retry count
                await self.redis_client.lpush(
                    self.queue_names["failed"],
                    json.dumps(failed_message)
                )
            else:
                # Move to permanent failed queue
                await self.redis_client.lpush(
                    self.queue_names["failed"],
                    json.dumps(failed_message)
                )

            logger.info(
                "message_failed",
                task_id=task_id,
                error=error,
                retry=retry
            )

        except Exception as e:
            logger.error(
                "fail_message_error",
                task_id=task_id,
                error=str(e)
            )

    async def get_queue_size(self, queue_name: str) -> int:
        """Get size of specific queue"""

        if not self.redis_client:
            return 0

        queue_key = self.queue_names.get(queue_name)
        if not queue_key:
            return 0

        try:
            return await self.redis_client.zcard(queue_key)
        except Exception as e:
            logger.error(
                "get_queue_size_error",
                queue=queue_name,
                error=str(e)
            )
            return 0

    async def get_all_queue_sizes(self) -> Dict[str, int]:
        """Get sizes of all queues"""

        sizes = {}
        for queue_name in self.queue_names.keys():
            sizes[queue_name] = await self.get_queue_size(queue_name)

        return sizes

    async def get_processing_count(self) -> int:
        """Get count of currently processing tasks"""

        if not self.redis_client:
            return 0

        try:
            return await self.redis_client.scard(self.processing_set)
        except Exception as e:
            logger.error(
                "get_processing_count_error",
                error=str(e)
            )
            return 0

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""

        stats = {
            "queue_sizes": await self.get_all_queue_sizes(),
            "processing_count": await self.get_processing_count(),
            "timestamp": time.time()
        }

        # Add processing rate (messages per minute)
        try:
            completed_key = self.queue_names["completed"]
            recent_completed = await self.redis_client.lrange(completed_key, 0, 59)  # Last 60

            if recent_completed:
                oldest_time = json.loads(recent_completed[-1])["completed_at"]
                time_window = time.time() - oldest_time

                if time_window > 0:
                    stats["processing_rate_per_minute"] = len(recent_completed) / (time_window / 60)
                else:
                    stats["processing_rate_per_minute"] = len(recent_completed)
            else:
                stats["processing_rate_per_minute"] = 0

        except Exception as e:
            logger.error(
                "get_queue_stats_error",
                error=str(e)
            )
            stats["processing_rate_per_minute"] = 0

        return stats

    async def retry_failed_messages(self, queue_name: str = "failed"):
        """Retry messages from failed queue"""

        if not self.redis_client:
            return 0

        queue_key = self.queue_names.get(queue_name)
        if not queue_key:
            raise ValueError(f"Unknown queue: {queue_name}")

        retried_count = 0

        try:
            # Get all failed messages
            failed_messages = await self.redis_client.lrange(queue_key, 0, -1)

            for failed_data in failed_messages:
                try:
                    failed_message = json.loads(failed_data)

                    if failed_message.get("retry", True):
                        # Re-queue to original queue
                        await self.add_message(
                            "processing",  # Or determine original queue from task
                            {
                                "task_id": failed_message["task_id"],
                                "data": {},  # Would need to track original data
                                "retry_count": 1  # Would need to track actual count
                            }
                        )
                        retried_count += 1

                except Exception as e:
                    logger.error(
                        "retry_message_error",
                        task_id=failed_message.get("task_id"),
                        error=str(e)
                    )

            # Clear failed queue if messages were retried
            if retried_count > 0:
                await self.redis_client.del(queue_key)

            logger.info(
                "failed_messages_retried",
                retried_count=retried_count
            )

        except Exception as e:
            logger.error(
                "retry_failed_messages_error",
                error=str(e)
            )

        return retried_count

    async def cleanup_old_messages(self, max_age_hours: int = 24):
        """Clean up old completed messages"""

        if not self.redis_client:
            return

        try:
            cutoff_time = time.time() - (max_age_hours * 3600)
            completed_key = self.queue_names["completed"]

            # Get and filter old messages
            all_completed = await self.redis_client.lrange(completed_key, 0, -1)
            valid_messages = []

            for message_data in all_completed:
                try:
                    completed_at = json.loads(message_data)["completed_at"]
                    if completed_at > cutoff_time:
                        valid_messages.append(message_data)
                except (json.JSONDecodeError, KeyError):
                    continue

            # Update list with only valid messages
            await self.redis_client.del(completed_key)
            if valid_messages:
                await self.redis_client.lpush(completed_key, *valid_messages)

            cleaned_count = len(all_completed) - len(valid_messages)
            logger.info(
                "old_messages_cleaned",
                cleaned_count=cleaned_count,
                max_age_hours=max_age_hours
            )

        except Exception as e:
            logger.error(
                "cleanup_old_messages_error",
                error=str(e)
            )

    def _calculate_priority_score(self, priority: int, timestamp: float) -> float:
        """Calculate priority score for sorted set"""
        # Lower score = higher priority
        # Priority 1 = most important, Priority 10 = least important
        # Include timestamp to maintain FIFO within same priority
        return (priority * 1000000000) + timestamp

    async def get_queue_health(self) -> Dict[str, Any]:
        """Get queue health information"""

        if not self.redis_client:
            return {"status": "disconnected"}

        try:
            # Test Redis connection
            await self.redis_client.ping()

            stats = await self.get_queue_stats()

            # Determine health status
            total_queued = sum(stats["queue_sizes"].values())
            processing_count = stats["processing_count"]

            health_status = "healthy"
            if total_queued > 1000:
                health_status = "warning"
            if total_queued > 5000:
                health_status = "critical"

            return {
                "status": health_status,
                "redis_connected": True,
                **stats
            }

        except Exception as e:
            return {
                "status": "error",
                "redis_connected": False,
                "error": str(e)
            }
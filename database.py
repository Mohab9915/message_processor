"""
Database Manager for Message Processor Service
Handles database operations for conversations, messages, and context
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, sessionmaker
from sqlalchemy.orm import selectinload
from sqlalchemy import select, update, delete, and_, or_, func, desc
from sqlalchemy.dialects.postgresql import insert
from typing import Dict, Any, List, Optional, Union
import json
import time
from datetime import datetime, timedelta

from shared.config.settings import get_settings
from shared.utils.logger import get_service_logger
from shared.models.database import (
    Conversation, Message, MessageIntent, ConversationContext,
    AIProviderUsage, ConversationAnalytics, PlatformType
)

settings = get_settings("message-processor")
logger = get_service_logger("database")


class DatabaseManager:
    """Database operations manager"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        self.session_factory = None

    async def initialize(self):
        """Initialize database connection"""
        try:
            self.engine = create_async_engine(
                self.database_url,
                echo=settings.debug,
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
                pool_timeout=settings.db_pool_timeout,
                pool_recycle=settings.db_pool_recycle,
                pool_pre_ping=True,
            )

            self.session_factory = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Test connection
            async with self.session_factory() as session:
                await session.execute("SELECT 1")
                await session.commit()

            logger.info("database_connection_established", url=self.database_url)

        except Exception as e:
            logger.error(
                "database_connection_failed",
                error=str(e),
                url=self.database_url,
                exc_info=True
            )
            raise

    async def cleanup(self):
        """Cleanup database connections"""
        if self.engine:
            await self.engine.dispose()
            logger.info("database_connections_cleaned")

    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self.engine is not None

    def get_pool_status(self) -> Dict[str, Any]:
        """Get connection pool status"""
        if not self.engine:
            return {"status": "not_initialized"}

        pool = self.engine.pool
        return {
            "status": "active",
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }

    async def get_session(self) -> AsyncSession:
        """Get database session"""
        return self.session_factory()

    # Conversation operations
    async def get_or_create_conversation(
        self,
        platform: str,
        platform_specific_id: str,
        platform_page_id: str = None
    ) -> Conversation:
        """Get existing conversation or create new one"""

        async with await self.get_session() as session:
            # Try to find existing conversation
            stmt = select(Conversation).where(
                and_(
                    Conversation.platform == platform,
                    Conversation.platform_specific_id == platform_specific_id
                )
            )

            result = await session.execute(stmt)
            conversation = result.scalar_one_or_none()

            if not conversation:
                # Create new conversation
                conversation = Conversation(
                    conversation_key=f"{platform}_{platform_specific_id}_{platform_page_id or 'default'}",
                    platform=PlatformType(platform),
                    platform_specific_id=platform_specific_id,
                    status="active",
                    current_stage="greeting",
                    metadata={
                        "platform_page_id": platform_page_id,
                        "created_by": "message_processor"
                    }
                )

                session.add(conversation)
                await session.commit()
                await session.refresh(conversation)

                logger.info(
                    "conversation_created",
                    conversation_id=str(conversation.id),
                    platform=platform,
                    platform_specific_id=platform_specific_id
                )

            return conversation

    async def update_conversation(
        self,
        conversation_id: str,
        updates: Dict[str, Any]
    ) -> Optional[Conversation]:
        """Update conversation"""

        async with await self.get_session() as session:
            stmt = update(Conversation).where(
                Conversation.id == conversation_id
            ).values(
                **updates,
                updated_at=datetime.utcnow()
            ).returning(Conversation)

            result = await session.execute(stmt)
            await session.commit()

            if result.rowcount > 0:
                updated_conversation = result.scalar_one()
                await session.refresh(updated_conversation)
                return updated_conversation

            return None

    async def get_conversation_context(
        self,
        conversation_id: str,
        context_type: str = "conversation"
    ) -> Dict[str, Any]:
        """Get conversation context"""

        async with await self.get_session() as session:
            stmt = select(ConversationContext).where(
                and_(
                    ConversationContext.conversation_id == conversation_id,
                    ConversationContext.context_type == context_type
                )
            )

            result = await session.execute(stmt)
            contexts = result.scalars().all()

            context_data = {}
            for context in contexts:
                context_data[context.context_key] = context.context_value

            return context_data

    async def set_conversation_context(
        self,
        conversation_id: str,
        context_key: str,
        context_value: Any,
        context_type: str = "conversation",
        expires_at: datetime = None
    ):
        """Set conversation context"""

        async with await self.get_session() as session:
            # Upsert context
            stmt = insert(ConversationContext).values(
                conversation_id=conversation_id,
                context_key=context_key,
                context_value=context_value,
                context_type=context_type,
                expires_at=expires_at,
                updated_at=datetime.utcnow()
            ).on_conflict_do_update(
                index_elements=['conversation_id', 'context_key', 'context_type'],
                set_={
                    'context_value': context_value,
                    'expires_at': expires_at,
                    'updated_at': datetime.utcnow()
                }
            )

            await session.execute(stmt)
            await session.commit()

    # Message operations
    async def create_message(self, message_data: Dict[str, Any]) -> Message:
        """Create new message"""

        async with await self.get_session() as session:
            message = Message(
                conversation_id=message_data["conversation_id"],
                platform_message_id=message_data.get("platform_message_id"),
                content=message_data["content"],
                message_type=message_data.get("message_type", "text"),
                direction=message_data.get("direction", "inbound"),
                sender_info=message_data.get("sender_info", {}),
                processing_metadata=message_data.get("processing_metadata", {}),
                ai_response=message_data.get("ai_response", {}),
                intent_result=message_data.get("intent_result", {}),
                entities=message_data.get("entities", {}),
                created_at=datetime.fromtimestamp(message_data.get("timestamp", time.time()))
            )

            session.add(message)
            await session.commit()
            await session.refresh(message)

            logger.info(
                "message_created",
                message_id=str(message.id),
                conversation_id=str(message.conversation_id),
                direction=message.direction
            )

            return message

    async def get_message(self, message_id: str) -> Optional[Message]:
        """Get message by ID"""

        async with await self.get_session() as session:
            stmt = select(Message).where(Message.id == message_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def update_message_status(
        self,
        message_id: str,
        status: str,
        error_message: str = None,
        processing_metadata: Dict[str, Any] = None
    ):
        """Update message processing status"""

        async with await self.get_session() as session:
            update_data = {
                "processed_at": datetime.utcnow()
            }

            if processing_metadata:
                current_metadata = processing_metadata
            else:
                # Get current metadata
                stmt = select(Message.processing_metadata).where(Message.id == message_id)
                result = await session.execute(stmt)
                current_metadata = result.scalar() or {}

            update_data["processing_metadata"] = {
                **current_metadata,
                "status": status,
                "updated_at": time.time(),
                **(processing_metadata or {})
            }

            if error_message:
                update_data["error_message"] = error_message

            stmt = update(Message).where(Message.id == message_id).values(**update_data)
            await session.execute(stmt)
            await session.commit()

    async def get_conversation_messages(
        self,
        conversation_id: str,
        limit: int = 50,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> List[Message]:
        """Get messages for conversation"""

        async with await self.get_session() as session:
            stmt = select(Message).where(Message.conversation_id == conversation_id)

            if before:
                stmt = stmt.where(Message.id < before)
            if after:
                stmt = stmt.where(Message.id > after)

            stmt = stmt.order_by(desc(Message.created_at)).limit(limit)

            result = await session.execute(stmt)
            return result.scalars().all()

    async def search_messages(
        self,
        query: str,
        conversation_id: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> List[Message]:
        """Search messages by content"""

        async with await self.get_session() as session:
            stmt = select(Message).where(
                Message.content.ilike(f"%{query}%")
            )

            if conversation_id:
                stmt = stmt.where(Message.conversation_id == conversation_id)

            # Note: Platform filtering would require joining with conversations
            # For simplicity, we'll implement it later

            stmt = stmt.order_by(desc(Message.created_at)).offset(offset).limit(limit)

            result = await session.execute(stmt)
            return result.scalars().all()

    # AI operations
    async def save_ai_result(
        self,
        message_id: str,
        ai_result: Dict[str, Any]
    ):
        """Save AI processing result"""

        async with await self.get_session() as session:
            # Save intent result
            if ai_result.get("intent"):
                intent = MessageIntent(
                    message_id=message_id,
                    intent_name=ai_result["intent"],
                    confidence=ai_result.get("confidence", 0.0),
                    entities=ai_result.get("entities", {}),
                    context=ai_result.get("context", {}),
                    processing_model=ai_result.get("model"),
                    processing_provider=ai_result.get("provider"),
                    processing_time_ms=ai_result.get("processing_time_ms"),
                    alternative_intents=ai_result.get("alternative_intents", [])
                )

                session.add(intent)

            # Update message with AI response
            if ai_result.get("response"):
                stmt = update(Message).where(Message.id == message_id).values(
                    ai_response={
                        "content": ai_result["response"],
                        "provider": ai_result.get("provider"),
                        "model": ai_result.get("model"),
                        "confidence": ai_result.get("confidence"),
                        "processing_time_ms": ai_result.get("processing_time_ms")
                    },
                    intent_result={
                        "intent": ai_result.get("intent"),
                        "confidence": ai_result.get("confidence")
                    },
                    entities=ai_result.get("entities", {}),
                    processed_at=datetime.utcnow()
                )

                await session.execute(stmt)

            # Save AI provider usage
            if ai_result.get("usage"):
                usage = AIProviderUsage(
                    provider=ai_result.get("provider"),
                    model=ai_result.get("model"),
                    message_id=message_id,
                    request_tokens=ai_result["usage"].get("request_tokens"),
                    response_tokens=ai_result["usage"].get("response_tokens"),
                    total_tokens=ai_result["usage"].get("total_tokens"),
                    cost_usd=ai_result["usage"].get("cost_usd"),
                    response_time_ms=ai_result.get("processing_time_ms"),
                    status="success"
                )

                session.add(usage)

            await session.commit()

    async def save_ai_error(
        self,
        message_id: str,
        provider: str,
        model: str,
        error: str,
        error_type: str = None
    ):
        """Save AI processing error"""

        async with await self.get_session() as session:
            usage = AIProviderUsage(
                provider=provider,
                model=model,
                message_id=message_id,
                status="error",
                error_message=error
            )

            session.add(usage)
            await session.commit()

    # Analytics operations
    async def record_analytics_event(
        self,
        conversation_id: str,
        event_type: str,
        event_data: Dict[str, Any]
    ):
        """Record analytics event"""

        async with await self.get_session() as session:
            analytics = ConversationAnalytics(
                conversation_id=conversation_id,
                event_type=event_type,
                event_data=event_data,
                created_at=datetime.utcnow()
            )

            session.add(analytics)
            await session.commit()

    async def get_conversation_analytics(
        self,
        conversation_id: str
    ) -> List[ConversationAnalytics]:
        """Get conversation analytics"""

        async with await self.get_session() as session:
            stmt = select(ConversationAnalytics).where(
                ConversationAnalytics.conversation_id == conversation_id
            ).order_by(desc(ConversationAnalytics.created_at))

            result = await session.execute(stmt)
            return result.scalars().all()

    # Maintenance operations
    async def cleanup_expired_context(self):
        """Clean up expired conversation context"""

        async with await self.get_session() as session:
            stmt = delete(ConversationContext).where(
                ConversationContext.expires_at < datetime.utcnow()
            )

            result = await session.execute(stmt)
            await session.commit()

            logger.info(
                "expired_context_cleaned",
                deleted_count=result.rowcount
            )

    async def get_conversation_statistics(
        self,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get conversation statistics"""

        async with await self.get_session() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=days)

            # Total conversations
            total_conv_stmt = select(func.count(Conversation.id)).where(
                Conversation.created_at >= cutoff_date
            )
            total_conversations = (await session.execute(total_conv_stmt)).scalar()

            # Messages per platform
            platform_stats = {}
            for platform in ["facebook", "whatsapp", "instagram"]:
                stmt = select(func.count(Message.id)).join(Conversation).where(
                    and_(
                        Conversation.platform == PlatformType(platform),
                        Message.created_at >= cutoff_date
                    )
                )
                count = (await session.execute(stmt)).scalar()
                platform_stats[platform] = count

            # Average response time
            avg_response_stmt = select(
                func.avg(Message.processing_time_ms)
            ).where(
                and_(
                    Message.direction == "inbound",
                    Message.processed_at.isnot(None),
                    Message.created_at >= cutoff_date
                )
            )
            avg_response_time = (await session.execute(avg_response_stmt)).scalar() or 0

            return {
                "total_conversations": total_conversations,
                "messages_by_platform": platform_stats,
                "avg_response_time_ms": float(avg_response_time),
                "period_days": days
            }
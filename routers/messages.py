"""
Messages Router
API endpoints for message management and processing
"""
from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import time
from datetime import datetime

router = APIRouter()


# Pydantic models
class MessageRequest(BaseModel):
    content: str = Field(..., description="Message content")
    conversation_id: Optional[str] = Field(None, description="Conversation ID")
    user_id: Optional[str] = Field(None, description="User ID")
    platform: str = Field(default="web", description="Platform source")
    message_type: str = Field(default="text", description="Message type")
    sender_info: Optional[Dict[str, Any]] = Field(None, description="Sender information")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class ProcessMessageRequest(BaseModel):
    message: str = Field(..., description="Message to process")
    user_id: str = Field(..., description="User ID")
    conversation_id: Optional[str] = Field(None, description="Conversation ID")
    platform: str = Field(default="web", description="Platform")
    context: Optional[Dict[str, Any]] = Field(None, description="Conversation context")
    options: Optional[Dict[str, Any]] = Field(None, description="Processing options")


# Global variables (will be injected by main app)
database_manager = None
ai_orchestrator = None
message_queue = None


async def get_database_manager():
    """Dependency to get database manager instance"""
    global database_manager
    if database_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database manager not available"
        )
    return database_manager


async def get_ai_orchestrator():
    """Dependency to get AI orchestrator instance"""
    global ai_orchestrator
    if ai_orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI orchestrator not available"
        )
    return ai_orchestrator


@router.get("/", summary="Get Messages")
async def get_messages(
    conversation_id: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: int = Field(default=50, ge=1, le=1000),
    offset: int = Field(default=0, ge=0),
    db_manager=Depends(get_database_manager)
):
    """
    Get messages with optional filtering.

    - **conversation_id**: Filter by conversation ID
    - **user_id**: Filter by user ID
    - **limit**: Maximum number of messages to return
    - **offset**: Number of messages to skip
    """
    try:
        messages = await db_manager.get_messages(
            conversation_id=conversation_id,
            user_id=user_id,
            limit=limit,
            offset=offset
        )

        return {
            "messages": messages,
            "count": len(messages),
            "limit": limit,
            "offset": offset,
            "filters": {
                "conversation_id": conversation_id,
                "user_id": user_id
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get messages: {str(e)}"
        )


@router.post("/", summary="Create Message")
async def create_message(
    request: MessageRequest,
    background_tasks: BackgroundTasks,
    db_manager=Depends(get_database_manager)
):
    """
    Create a new message.
    """
    try:
        message_data = {
            "content": request.content,
            "conversation_id": request.conversation_id,
            "user_id": request.user_id,
            "platform": request.platform,
            "message_type": request.message_type,
            "sender_info": request.sender_info or {},
            "metadata": request.metadata or {},
            "direction": "inbound"
        }

        # Create message in database
        message = await db_manager.create_message(message_data)

        # Queue for processing
        if message_queue:
            processing_task = {
                "task_id": f"process_{message.id}",
                "task_type": "message_processing",
                "message_data": {
                    **message_data,
                    "message_id": str(message.id)
                },
                "created_at": time.time()
            }
            await message_queue.add_message("processing", processing_task)

        return {
            "message_id": str(message.id),
            "created": True,
            "queued_for_processing": True,
            "content": request.content,
            "timestamp": message.created_at.isoformat()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create message: {str(e)}"
        )


@router.get("/{message_id}", summary="Get Message by ID")
async def get_message(
    message_id: str,
    db_manager=Depends(get_database_manager)
):
    """
    Get a specific message by ID.
    """
    try:
        message = await db_manager.get_message(message_id)

        if not message:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Message {message_id} not found"
            )

        return message

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get message: {str(e)}"
        )


@router.post("/process", summary="Process Message with AI")
async def process_message(
    request: ProcessMessageRequest,
    background_tasks: BackgroundTasks,
    ai_service=Depends(get_ai_orchestrator),
    db_manager=Depends(get_database_manager)
):
    """
    Process a message with AI analysis and generate response.
    """
    try:
        # Create message
        message_data = {
            "content": request.message,
            "user_id": request.user_id,
            "conversation_id": request.conversation_id,
            "platform": request.platform,
            "context": request.context or {},
            "options": request.options or {}
        }

        # Process with AI
        ai_result = await ai_service.process_message(
            message=request.message,
            context=request.context or {},
            platform=request.platform
        )

        # Store message and result
        message = await db_manager.create_message({
            "content": request.message,
            "user_id": request.user_id,
            "conversation_id": request.conversation_id,
            "platform": request.platform,
            "direction": "inbound",
            "ai_result": ai_result
        })

        # Generate response if AI provided one
        response_data = None
        if ai_result.get("response"):
            response_data = await db_manager.create_message({
                "content": ai_result["response"],
                "user_id": request.user_id,
                "conversation_id": request.conversation_id,
                "platform": request.platform,
                "direction": "outbound",
                "in_reply_to": str(message.id),
                "ai_metadata": ai_result
            })

        return {
            "message_id": str(message.id),
            "processed": True,
            "ai_result": ai_result,
            "response": {
                "response_id": str(response_data.id) if response_data else None,
                "content": ai_result.get("response"),
                "generated": response_data is not None
            },
            "processing_time_ms": ai_result.get("processing_time_ms", 0),
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process message: {str(e)}"
        )


@router.get("/conversation/{conversation_id}/history", summary="Get Conversation History")
async def get_conversation_history(
    conversation_id: str,
    limit: int = Field(default=50, ge=1, le=1000),
    include_ai_results: bool = True,
    db_manager=Depends(get_database_manager)
):
    """
    Get message history for a conversation.
    """
    try:
        messages = await db_manager.get_conversation_messages(
            conversation_id=conversation_id,
            limit=limit,
            include_ai_results=include_ai_results
        )

        return {
            "conversation_id": conversation_id,
            "messages": messages,
            "count": len(messages),
            "limit": limit,
            "include_ai_results": include_ai_results
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get conversation history: {str(e)}"
        )


@router.delete("/{message_id}", summary="Delete Message")
async def delete_message(
    message_id: str,
    db_manager=Depends(get_database_manager)
):
    """
    Delete a message.
    """
    try:
        success = await db_manager.delete_message(message_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Message {message_id} not found"
            )

        return {"deleted": True, "message_id": message_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete message: {str(e)}"
        )


@router.get("/stats/overview", summary="Get Message Statistics")
async def get_message_stats(
    conversation_id: Optional[str] = None,
    user_id: Optional[str] = None,
    days: int = Field(default=7, ge=1, le=365),
    db_manager=Depends(get_database_manager)
):
    """
    Get message processing statistics.
    """
    try:
        stats = await db_manager.get_message_statistics(
            conversation_id=conversation_id,
            user_id=user_id,
            days=days
        )

        return stats

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get message statistics: {str(e)}"
        )
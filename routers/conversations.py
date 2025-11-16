"""
Conversations Router
API endpoints for conversation management
"""
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import time
from datetime import datetime

router = APIRouter()


# Pydantic models
class CreateConversationRequest(BaseModel):
    user_id: str = Field(..., description="User ID")
    platform: str = Field(default="web", description="Platform")
    platform_user_id: Optional[str] = Field(None, description="Platform-specific user ID")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class UpdateConversationRequest(BaseModel):
    status: Optional[str] = Field(None, description="Conversation status")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Update metadata")
    tags: Optional[List[str]] = Field(None, description="Conversation tags")


# Global variables (will be injected by main app)
database_manager = None


async def get_database_manager():
    """Dependency to get database manager instance"""
    global database_manager
    if database_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database manager not available"
        )
    return database_manager


@router.get("/", summary="Get Conversations")
async def get_conversations(
    user_id: Optional[str] = None,
    platform: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Field(default=50, ge=1, le=1000),
    offset: int = Field(default=0, ge=0),
    db_manager=Depends(get_database_manager)
):
    """
    Get conversations with optional filtering.

    - **user_id**: Filter by user ID
    - **platform**: Filter by platform
    - **status**: Filter by status
    - **limit**: Maximum number of conversations to return
    - **offset**: Number of conversations to skip
    """
    try:
        conversations = await db_manager.get_conversations(
            user_id=user_id,
            platform=platform,
            status=status,
            limit=limit,
            offset=offset
        )

        return {
            "conversations": conversations,
            "count": len(conversations),
            "limit": limit,
            "offset": offset,
            "filters": {
                "user_id": user_id,
                "platform": platform,
                "status": status
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get conversations: {str(e)}"
        )


@router.post("/", summary="Create Conversation")
async def create_conversation(
    request: CreateConversationRequest,
    db_manager=Depends(get_database_manager)
):
    """
    Create a new conversation.
    """
    try:
        conversation_data = {
            "user_id": request.user_id,
            "platform": request.platform,
            "platform_user_id": request.platform_user_id,
            "metadata": request.metadata or {},
            "status": "active"
        }

        conversation = await db_manager.create_conversation(conversation_data)

        return {
            "conversation_id": str(conversation.id),
            "created": True,
            "user_id": request.user_id,
            "platform": request.platform,
            "status": conversation.status,
            "created_at": conversation.created_at.isoformat()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create conversation: {str(e)}"
        )


@router.get("/{conversation_id}", summary="Get Conversation by ID")
async def get_conversation(
    conversation_id: str,
    include_messages: bool = False,
    message_limit: int = Field(default=10, ge=1, le=100),
    db_manager=Depends(get_database_manager)
):
    """
    Get a specific conversation by ID.
    """
    try:
        conversation = await db_manager.get_conversation(conversation_id)

        if not conversation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation {conversation_id} not found"
            )

        result = conversation

        # Include recent messages if requested
        if include_messages:
            messages = await db_manager.get_conversation_messages(
                conversation_id=conversation_id,
                limit=message_limit
            )
            result["messages"] = messages
            result["message_count"] = len(messages)

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get conversation: {str(e)}"
        )


@router.put("/{conversation_id}", summary="Update Conversation")
async def update_conversation(
    conversation_id: str,
    request: UpdateConversationRequest,
    db_manager=Depends(get_database_manager)
):
    """
    Update conversation information.
    """
    try:
        update_data = {}
        if request.status is not None:
            update_data["status"] = request.status
        if request.metadata is not None:
            update_data["metadata"] = request.metadata
        if request.tags is not None:
            update_data["tags"] = request.tags

        success = await db_manager.update_conversation(conversation_id, update_data)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation {conversation_id} not found"
            )

        return {"updated": True, "conversation_id": conversation_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update conversation: {str(e)}"
        )


@router.delete("/{conversation_id}", summary="Delete Conversation")
async def delete_conversation(
    conversation_id: str,
    db_manager=Depends(get_database_manager)
):
    """
    Delete a conversation and all its messages.
    """
    try:
        success = await db_manager.delete_conversation(conversation_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation {conversation_id} not found"
            )

        return {"deleted": True, "conversation_id": conversation_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete conversation: {str(e)}"
        )


@router.get("/user/{user_id}/conversations", summary="Get User Conversations")
async def get_user_conversations(
    user_id: str,
    platform: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Field(default=50, ge=1, le=1000),
    db_manager=Depends(get_database_manager)
):
    """
    Get all conversations for a specific user.
    """
    try:
        conversations = await db_manager.get_conversations(
            user_id=user_id,
            platform=platform,
            status=status,
            limit=limit
        )

        return {
            "user_id": user_id,
            "conversations": conversations,
            "count": len(conversations),
            "filters": {
                "platform": platform,
                "status": status
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user conversations: {str(e)}"
        )


@router.get("/{conversation_id}/summary", summary="Get Conversation Summary")
async def get_conversation_summary(
    conversation_id: str,
    db_manager=Depends(get_database_manager)
):
    """
    Get a summary of conversation including message count, duration, etc.
    """
    try:
        summary = await db_manager.get_conversation_summary(conversation_id)

        if not summary:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation {conversation_id} not found"
            )

        return summary

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get conversation summary: {str(e)}"
        )


@router.get("/stats/overview", summary="Get Conversation Statistics")
async def get_conversation_stats(
    days: int = Field(default=30, ge=1, le=365),
    platform: Optional[str] = None,
    db_manager=Depends(get_database_manager)
):
    """
    Get conversation statistics.
    """
    try:
        stats = await db_manager.get_conversation_statistics(
            days=days,
            platform=platform
        )

        return stats

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get conversation statistics: {str(e)}"
        )
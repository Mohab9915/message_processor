"""
Webhooks Router
API endpoints for webhook management and processing
"""
from fastapi import APIRouter, HTTPException, status, Depends, Request, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import time
import json

router = APIRouter()


# Pydantic models
class WebhookEvent(BaseModel):
    platform: str = Field(..., description="Platform name")
    event_type: str = Field(..., description="Event type")
    data: Dict[str, Any] = Field(..., description="Event data")
    timestamp: Optional[float] = Field(None, description="Event timestamp")
    signature: Optional[str] = Field(None, description="Webhook signature")
    headers: Optional[Dict[str, str]] = Field(None, description="Request headers")


class WebhookResponse(BaseModel):
    status: str = Field(..., description="Response status")
    message: str = Field(..., description="Response message")
    processed: bool = Field(default=False, description="Whether event was processed")
    data: Optional[Dict[str, Any]] = Field(None, description="Response data")


# Global variables (will be injected by main app)
message_queue = None


async def get_message_queue():
    """Dependency to get message queue instance"""
    global message_queue
    if message_queue is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message queue not available"
        )
    return message_queue


@router.post("/facebook", summary="Facebook Webhook")
async def facebook_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    msg_queue=Depends(get_message_queue)
):
    """
    Handle Facebook Messenger webhooks.
    """
    try:
        # Get request data
        body = await request.body()
        headers = dict(request.headers)

        # Parse webhook data
        try:
            webhook_data = json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON in webhook body"
            )

        # Verify webhook signature if provided
        signature = headers.get("x-hub-signature-256")
        if signature:
            # TODO: Implement signature verification
            pass

        # Process webhook entry
        if "entry" in webhook_data:
            for entry in webhook_data["entry"]:
                if "messaging" in entry:
                    for messaging_event in entry["messaging"]:
                        await process_facebook_messaging_event(
                            messaging_event,
                            msg_queue,
                            background_tasks
                        )

        return WebhookResponse(
            status="success",
            message="Facebook webhook processed",
            processed=True
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process Facebook webhook: {str(e)}"
        )


@router.post("/whatsapp", summary="WhatsApp Webhook")
async def whatsapp_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    msg_queue=Depends(get_message_queue)
):
    """
    Handle WhatsApp webhooks.
    """
    try:
        # Get request data
        body = await request.body()
        headers = dict(request.headers)

        # Parse webhook data
        try:
            webhook_data = json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON in webhook body"
            )

        # Process WhatsApp messages
        if "message" in webhook_data:
            await process_whatsapp_message(
                webhook_data["message"],
                msg_queue,
                background_tasks
            )

        return WebhookResponse(
            status="success",
            message="WhatsApp webhook processed",
            processed=True
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process WhatsApp webhook: {str(e)}"
        )


@router.get("/facebook", summary="Facebook Webhook Verification")
async def facebook_webhook_verify(
    hub_mode: str = Field(None, alias="hub.mode"),
    hub_challenge: str = Field(None, alias="hub.challenge"),
    hub_verify_token: str = Field(None, alias="hub.verify_token")
):
    """
    Verify Facebook webhook subscription.
    """
    if hub_mode == "subscribe" and hub_verify_token:
        # TODO: Verify token with stored token
        # For now, just return the challenge
        return int(hub_challenge)

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Webhook verification failed"
    )


@router.get("/whatsapp", summary="WhatsApp Webhook Verification")
async def whatsapp_webhook_verify():
    """
    Handle WhatsApp webhook verification.
    """
    return {"status": "WhatsApp webhook endpoint is active"}


@router.post("/generic", summary="Generic Webhook")
async def generic_webhook(
    event: WebhookEvent,
    background_tasks: BackgroundTasks,
    msg_queue=Depends(get_message_queue)
):
    """
    Handle generic webhook events.
    """
    try:
        # Queue webhook event for processing
        webhook_task = {
            "task_id": f"webhook_{event.platform}_{int(time.time())}",
            "task_type": "webhook_processing",
            "webhook_event": {
                "platform": event.platform,
                "event_type": event.event_type,
                "data": event.data,
                "timestamp": event.timestamp or time.time(),
                "signature": event.signature,
                "headers": event.headers
            },
            "created_at": time.time()
        }

        await msg_queue.add_message("webhooks", webhook_task)

        return WebhookResponse(
            status="success",
            message="Generic webhook queued for processing",
            processed=True,
            data={"task_id": webhook_task["task_id"]}
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process generic webhook: {str(e)}"
        )


@router.get("/events", summary="Get Webhook Events")
async def get_webhook_events(
    platform: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = Field(default=50, ge=1, le=1000),
    msg_queue=Depends(get_message_queue)
):
    """
    Get webhook events (for debugging and monitoring).
    """
    try:
        # This would typically query a database
        # For now, return a placeholder
        return {
            "events": [],
            "count": 0,
            "filters": {
                "platform": platform,
                "event_type": event_type,
                "limit": limit
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get webhook events: {str(e)}"
        )


@router.post("/test", summary="Test Webhook")
async def test_webhook(
    platform: str = Field(..., description="Platform to test"),
    event_type: str = Field(default="message", description="Event type to test"),
    msg_queue=Depends(get_message_queue)
):
    """
    Test webhook processing with sample data.
    """
    try:
        test_event = {
            "platform": platform,
            "event_type": event_type,
            "data": {
                "test": True,
                "message": "This is a test webhook event",
                "timestamp": time.time()
            }
        }

        # Queue test event
        test_task = {
            "task_id": f"test_{platform}_{event_type}_{int(time.time())}",
            "task_type": "webhook_test",
            "webhook_event": test_event,
            "created_at": time.time()
        }

        await msg_queue.add_message("test", test_task)

        return {
            "status": "success",
            "message": "Test webhook queued",
            "test_event": test_event,
            "task_id": test_task["task_id"]
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test webhook: {str(e)}"
        )


# Helper functions
async def process_facebook_messaging_event(
    messaging_event: Dict[str, Any],
    msg_queue,
    background_tasks: BackgroundTasks
):
    """Process Facebook messaging event"""
    try:
        # Extract message data
        sender_id = messaging_event.get("sender", {}).get("id")
        recipient_id = messaging_event.get("recipient", {}).get("id")

        if "message" in messaging_event:
            message_text = messaging_event["message"].get("text")
            message_id = messaging_event["message"].get("mid")

            if message_text:
                # Create message processing task
                message_task = {
                    "task_id": f"fb_message_{message_id}",
                    "task_type": "message_processing",
                    "message_data": {
                        "platform": "facebook",
                        "platform_user_id": sender_id,
                        "recipient_id": recipient_id,
                        "content": message_text,
                        "message_id": message_id,
                        "timestamp": time.time()
                    },
                    "created_at": time.time()
                }

                await msg_queue.add_message("processing", message_task)

        # Handle other event types (postbacks, deliveries, etc.)
        # TODO: Implement additional event handling

    except Exception as e:
        # Log error but don't raise exception to avoid webhook failures
        print(f"Error processing Facebook messaging event: {e}")


async def process_whatsapp_message(
    message_data: Dict[str, Any],
    msg_queue,
    background_tasks: BackgroundTasks
):
    """Process WhatsApp message"""
    try:
        # Extract message data
        sender_id = message_data.get("from")
        message_text = message_data.get("text", {}).get("body")
        message_id = message_data.get("id")

        if message_text:
            # Create message processing task
            message_task = {
                "task_id": f"wa_message_{message_id}",
                "task_type": "message_processing",
                "message_data": {
                    "platform": "whatsapp",
                    "platform_user_id": sender_id,
                    "content": message_text,
                    "message_id": message_id,
                    "timestamp": time.time()
                },
                "created_at": time.time()
            }

            await msg_queue.add_message("processing", message_task)

    except Exception as e:
        # Log error but don't raise exception
        print(f"Error processing WhatsApp message: {e}")
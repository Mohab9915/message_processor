"""
AI Processing Router
API endpoints for AI processing and orchestration
"""
from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import time

router = APIRouter()


# Pydantic models
class AIProcessRequest(BaseModel):
    text: str = Field(..., description="Text to process")
    user_id: Optional[str] = Field(None, description="User ID")
    conversation_id: Optional[str] = Field(None, description="Conversation ID")
    platform: str = Field(default="web", description="Platform")
    language: Optional[str] = Field(None, description="Language code")
    context: Optional[Dict[str, Any]] = Field(None, description="Conversation context")
    options: Optional[Dict[str, Any]] = Field(None, description="Processing options")


class IntentRequest(BaseModel):
    text: str = Field(..., description="Text to classify")
    language: Optional[str] = Field(None, description="Language code")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class EntityRequest(BaseModel):
    text: str = Field(..., description="Text to extract entities from")
    entity_types: Optional[List[str]] = Field(None, description="Specific entity types to extract")
    language: Optional[str] = Field(None, description="Language code")


class SentimentRequest(BaseModel):
    text: str = Field(..., description="Text to analyze")
    language: Optional[str] = Field(None, description="Language code")
    detailed: bool = Field(default=False, description="Return detailed sentiment analysis")


class GenerateRequest(BaseModel):
    prompt: str = Field(..., description="Generation prompt")
    context: Optional[Dict[str, Any]] = Field(None, description="Context for generation")
    max_tokens: Optional[int] = Field(default=500, ge=1, le=2000)
    temperature: Optional[float] = Field(default=0.7, ge=0.0, le=2.0)
    user_id: Optional[str] = Field(None, description="User ID")


# Global variables (will be injected by main app)
ai_orchestrator = None


async def get_ai_orchestrator():
    """Dependency to get AI orchestrator instance"""
    global ai_orchestrator
    if ai_orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI orchestrator not available"
        )
    return ai_orchestrator


@router.post("/process", summary="Process Text with AI")
async def process_text(
    request: AIProcessRequest,
    background_tasks: BackgroundTasks,
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Process text with comprehensive AI analysis.
    """
    try:
        # Process with AI orchestrator
        result = await ai_service.process_text(
            text=request.text,
            context=request.context or {},
            platform=request.platform,
            language=request.language,
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            options=request.options or {}
        )

        return {
            "processed": True,
            "text": request.text,
            "results": result,
            "processing_time_ms": result.get("processing_time_ms", 0),
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process text: {str(e)}"
        )


@router.post("/intent", summary="Classify Intent")
async def classify_intent(
    request: IntentRequest,
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Classify the intent of the given text.
    """
    try:
        result = await ai_service.classify_intent(
            text=request.text,
            context=request.context or {},
            language=request.language
        )

        return {
            "text": request.text,
            "intent": result.get("intent"),
            "confidence": result.get("confidence", 0.0),
            "alternatives": result.get("alternatives", []),
            "processing_time_ms": result.get("processing_time_ms", 0)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to classify intent: {str(e)}"
        )


@router.post("/entities", summary="Extract Entities")
async def extract_entities(
    request: EntityRequest,
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Extract entities from the given text.
    """
    try:
        result = await ai_service.extract_entities(
            text=request.text,
            entity_types=request.entity_types,
            language=request.language
        )

        return {
            "text": request.text,
            "entities": result.get("entities", []),
            "entity_count": len(result.get("entities", [])),
            "processing_time_ms": result.get("processing_time_ms", 0)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to extract entities: {str(e)}"
        )


@router.post("/sentiment", summary="Analyze Sentiment")
async def analyze_sentiment(
    request: SentimentRequest,
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Analyze sentiment of the given text.
    """
    try:
        result = await ai_service.analyze_sentiment(
            text=request.text,
            language=request.language,
            detailed=request.detailed
        )

        return {
            "text": request.text,
            "sentiment": result.get("sentiment"),
            "confidence": result.get("confidence", 0.0),
            "polarity": result.get("polarity", 0.0),
            "subjectivity": result.get("subjectivity", 0.0),
            "emotions": result.get("emotions", {}),
            "detailed_analysis": result.get("detailed", {}),
            "processing_time_ms": result.get("processing_time_ms", 0)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze sentiment: {str(e)}"
        )


@router.post("/generate", summary="Generate Response")
async def generate_response(
    request: GenerateRequest,
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Generate AI response based on prompt and context.
    """
    try:
        result = await ai_service.generate_response(
            prompt=request.prompt,
            context=request.context or {},
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            user_id=request.user_id
        )

        return {
            "prompt": request.prompt,
            "response": result.get("response"),
            "model_used": result.get("model"),
            "provider": result.get("provider"),
            "tokens_used": result.get("tokens_used", 0),
            "processing_time_ms": result.get("processing_time_ms", 0),
            "metadata": result.get("metadata", {})
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate response: {str(e)}"
        )


@router.post("/translate", summary="Translate Text")
async def translate_text(
    text: str = Field(..., description="Text to translate"),
    target_language: str = Field(..., description="Target language code"),
    source_language: Optional[str] = Field(None, description="Source language code"),
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Translate text to target language.
    """
    try:
        result = await ai_service.translate_text(
            text=text,
            target_language=target_language,
            source_language=source_language
        )

        return {
            "original_text": text,
            "translated_text": result.get("translated_text"),
            "source_language": result.get("source_language"),
            "target_language": target_language,
            "confidence": result.get("confidence", 0.0),
            "processing_time_ms": result.get("processing_time_ms", 0)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to translate text: {str(e)}"
        )


@router.get("/models", summary="Get Available Models")
async def get_available_models(
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Get list of available AI models and providers.
    """
    try:
        models = await ai_service.get_available_models()

        return {
            "models": models,
            "count": len(models),
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get available models: {str(e)}"
        )


@router.get("/providers", summary="Get AI Providers Status")
async def get_providers_status(
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Get status of all AI providers.
    """
    try:
        status = await ai_service.get_providers_status()

        return {
            "providers": status,
            "healthy_providers": sum(1 for p in status.values() if p.get("healthy", False)),
            "total_providers": len(status),
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get providers status: {str(e)}"
        )


@router.post("/batch", summary="Batch Process Texts")
async def batch_process(
    texts: List[str] = Field(..., description="List of texts to process"),
    operations: List[str] = Field(default=["intent", "entities", "sentiment"], description="Operations to perform"),
    ai_service=Depends(get_ai_orchestrator)
):
    """
    Process multiple texts with batch operations.
    """
    try:
        if len(texts) > 100:  # Limit batch size
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch size cannot exceed 100 texts"
            )

        results = []
        for text in texts:
            text_results = {}

            if "intent" in operations:
                intent_result = await ai_service.classify_intent(text=text)
                text_results["intent"] = intent_result

            if "entities" in operations:
                entities_result = await ai_service.extract_entities(text=text)
                text_results["entities"] = entities_result

            if "sentiment" in operations:
                sentiment_result = await ai_service.analyze_sentiment(text=text)
                text_results["sentiment"] = sentiment_result

            results.append({
                "text": text,
                "results": text_results
            })

        return {
            "batch_size": len(texts),
            "operations": operations,
            "results": results,
            "processing_time_ms": time.time()
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to batch process texts: {str(e)}"
        )
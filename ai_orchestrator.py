"""
AI Orchestrator - Multi-Provider AI Processing
Orchestrates AI requests across multiple providers with fallback logic
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
import asyncio
import time
import json
import httpx

from shared.config.settings import get_settings, AIProviderConfig
from shared.utils.logger import get_service_logger, AIProviderLogger

settings = get_settings("message-processor")
logger = get_service_logger("ai_orchestrator")
ai_logger = AIProviderLogger(logger)


class AIProvider(ABC):
    """Base class for AI providers"""

    def __init__(self, provider_name: str, config: Dict[str, Any]):
        self.provider_name = provider_name
        self.config = config
        self.is_available = False

    @abstractmethod
    async def initialize(self):
        """Initialize provider"""
        pass

    @abstractmethod
    async def generate_response(
        self,
        prompt: str,
        context: Dict[str, Any] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Generate AI response"""
        pass

    @abstractmethod
    async def analyze_intent(
        self,
        message: str,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Analyze message intent"""
        pass

    async def health_check(self) -> bool:
        """Check provider health"""
        try:
            # Simple test request
            result = await self.generate_response("Hello", context={}, max_tokens=10)
            return bool(result.get("response"))
        except Exception as e:
            logger.warning(
                "ai_provider_health_check_failed",
                provider=self.provider_name,
                error=str(e)
            )
            return False


class OpenAIProvider(AIProvider):
    """OpenAI GPT provider"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("openai", config)
        self.api_key = config.get("api_key")
        self.default_model = config.get("model", "gpt-4")
        self.base_url = "https://api.openai.com/v1"

    async def initialize(self):
        """Initialize OpenAI client"""
        try:
            self.client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=self.config.get("timeout", 30)
            )

            # Test connection
            await self.health_check()
            self.is_available = True

            logger.info("openai_provider_initialized")

        except Exception as e:
            logger.error(
                "openai_provider_initialization_failed",
                error=str(e)
            )
            self.is_available = False

    async def generate_response(
        self,
        prompt: str,
        context: Dict[str, Any] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Generate response using OpenAI"""

        if not self.is_available:
            raise Exception("OpenAI provider not available")

        messages = self._build_messages(prompt, context, **kwargs)

        payload = {
            "model": kwargs.get("model", self.default_model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.get("max_tokens", 1000)),
            "temperature": kwargs.get("temperature", self.config.get("temperature", 0.7)),
        }

        start_time = time.time()

        try:
            response = await self.client.post("/chat/completions", json=payload)
            response.raise_for_status()

            result = response.json()
            processing_time = (time.time() - start_time) * 1000

            message_content = result["choices"][0]["message"]["content"]
            usage = result.get("usage", {})

            ai_logger.log_ai_response(
                provider="openai",
                model=payload["model"],
                response_time=processing_time,
                request_tokens=usage.get("prompt_tokens"),
                response_tokens=usage.get("completion_tokens"),
                total_tokens=usage.get("total_tokens"),
                cost=self._calculate_cost(usage)
            )

            return {
                "response": message_content,
                "provider": "openai",
                "model": payload["model"],
                "usage": {
                    "request_tokens": usage.get("prompt_tokens"),
                    "response_tokens": usage.get("completion_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                    "cost_usd": self._calculate_cost(usage)
                },
                "processing_time_ms": processing_time
            }

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            ai_logger.log_ai_error(
                provider="openai",
                model=payload.get("model", self.default_model),
                error=str(e),
                retry_count=kwargs.get("retry_count", 0)
            )
            raise

    async def analyze_intent(
        self,
        message: str,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Analyze intent using OpenAI function calling"""

        if not self.is_available:
            raise Exception("OpenAI provider not available")

        system_prompt = """
        You are an intent analysis system. Analyze the message and determine:
        1. The primary intent (greeting, price_inquiry, order_status, complaint, etc.)
        2. Confidence level (0.0 to 1.0)
        3. Extracted entities (products, colors, sizes, etc.)
        4. Language (english, arabic, hebrew)

        Respond with JSON format.
        """

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Analyze this message: {message}"}
        ]

        if context:
            messages.insert(1, {
                "role": "system",
                "content": f"Conversation context: {json.dumps(context)}"
            })

        payload = {
            "model": self.default_model,
            "messages": messages,
            "max_tokens": 500,
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }

        start_time = time.time()

        try:
            response = await self.client.post("/chat/completions", json=payload)
            response.raise_for_status()

            result = response.json()
            processing_time = (time.time() - start_time) * 1000

            analysis = json.loads(result["choices"][0]["message"]["content"])

            return {
                "intent": analysis.get("intent", "unknown"),
                "confidence": float(analysis.get("confidence", 0.5)),
                "entities": analysis.get("entities", {}),
                "language": analysis.get("language", "english"),
                "provider": "openai",
                "model": self.default_model,
                "processing_time_ms": processing_time
            }

        except Exception as e:
            ai_logger.log_ai_error(
                provider="openai",
                model=self.default_model,
                error=str(e),
                retry_count=0
            )
            raise

    def _build_messages(
        self,
        prompt: str,
        context: Dict[str, Any] = None,
        **kwargs
    ) -> List[Dict[str, str]]:
        """Build message list for OpenAI API"""

        messages = [
            {
                "role": "system",
                "content": kwargs.get(
                    "system_prompt",
                    "You are a helpful customer service assistant for an e-commerce platform. "
                    "Be friendly, helpful, and provide accurate information."
                )
            }
        ]

        if context:
            context_msg = "Conversation context:\n"
            if context.get("recent_messages"):
                context_msg += "Recent messages:\n"
                for msg in context["recent_messages"][-3:]:  # Last 3 messages
                    context_msg += f"- {msg.get('direction', 'unknown')}: {msg.get('content', '')}\n"

            if context.get("customer_info"):
                context_msg += f"Customer info: {context['customer_info']}\n"

            messages.append({"role": "system", "content": context_msg})

        messages.append({"role": "user", "content": prompt})

        return messages

    def _calculate_cost(self, usage: Dict[str, int]) -> float:
        """Calculate cost based on token usage"""
        # OpenAI GPT-4 pricing (approximate)
        prompt_cost = usage.get("prompt_tokens", 0) * 0.00001  # $0.01 per 1K prompt tokens
        completion_cost = usage.get("completion_tokens", 0) * 0.00003  # $0.03 per 1K completion tokens
        return prompt_cost + completion_cost


class DeepSeekProvider(AIProvider):
    """DeepSeek AI provider"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("deepseek", config)
        self.api_key = config.get("api_key")
        self.base_url = "https://api.deepseek.com/v1"

    async def initialize(self):
        """Initialize DeepSeek client"""
        try:
            self.client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=self.config.get("timeout", 30)
            )

            await self.health_check()
            self.is_available = True

            logger.info("deepseek_provider_initialized")

        except Exception as e:
            logger.error(
                "deepseek_provider_initialization_failed",
                error=str(e)
            )
            self.is_available = False

    async def generate_response(
        self,
        prompt: str,
        context: Dict[str, Any] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Generate response using DeepSeek"""

        if not self.is_available:
            raise Exception("DeepSeek provider not available")

        messages = self._build_messages(prompt, context, **kwargs)

        payload = {
            "model": kwargs.get("model", "deepseek-chat"),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.get("max_tokens", 1000)),
            "temperature": kwargs.get("temperature", self.config.get("temperature", 0.7)),
        }

        start_time = time.time()

        try:
            response = await self.client.post("/chat/completions", json=payload)
            response.raise_for_status()

            result = response.json()
            processing_time = (time.time() - start_time) * 1000

            message_content = result["choices"][0]["message"]["content"]
            usage = result.get("usage", {})

            ai_logger.log_ai_response(
                provider="deepseek",
                model=payload["model"],
                response_time=processing_time,
                request_tokens=usage.get("prompt_tokens"),
                response_tokens=usage.get("completion_tokens"),
                total_tokens=usage.get("total_tokens"),
                cost=self._calculate_cost(usage)
            )

            return {
                "response": message_content,
                "provider": "deepseek",
                "model": payload["model"],
                "usage": {
                    "request_tokens": usage.get("prompt_tokens"),
                    "response_tokens": usage.get("completion_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                    "cost_usd": self._calculate_cost(usage)
                },
                "processing_time_ms": processing_time
            }

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            ai_logger.log_ai_error(
                provider="deepseek",
                model=payload.get("model", "deepseek-chat"),
                error=str(e),
                retry_count=kwargs.get("retry_count", 0)
            )
            raise

    async def analyze_intent(
        self,
        message: str,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Analyze intent using DeepSeek"""

        # Similar implementation to OpenAI but with DeepSeek API
        system_prompt = """
        Analyze the message and return JSON with:
        {
          "intent": "primary_intent",
          "confidence": 0.0-1.0,
          "entities": {},
          "language": "english"
        }
        """

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Analyze: {message}"}
        ]

        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "max_tokens": 300,
            "temperature": 0.1
        }

        start_time = time.time()

        try:
            response = await self.client.post("/chat/completions", json=payload)
            response.raise_for_status()

            result = response.json()
            processing_time = (time.time() - start_time) * 1000

            try:
                analysis = json.loads(result["choices"][0]["message"]["content"])
            except json.JSONDecodeError:
                # Fallback parsing
                analysis = {
                    "intent": "unknown",
                    "confidence": 0.5,
                    "entities": {},
                    "language": "english"
                }

            return {
                "intent": analysis.get("intent", "unknown"),
                "confidence": float(analysis.get("confidence", 0.5)),
                "entities": analysis.get("entities", {}),
                "language": analysis.get("language", "english"),
                "provider": "deepseek",
                "model": "deepseek-chat",
                "processing_time_ms": processing_time
            }

        except Exception as e:
            ai_logger.log_ai_error(
                provider="deepseek",
                model="deepseek-chat",
                error=str(e),
                retry_count=0
            )
            raise

    def _build_messages(
        self,
        prompt: str,
        context: Dict[str, Any] = None,
        **kwargs
    ) -> List[Dict[str, str]]:
        """Build message list for DeepSeek API"""
        # Similar to OpenAI implementation
        return [
            {"role": "system", "content": "You are a helpful customer service assistant."},
            {"role": "user", "content": prompt}
        ]

    def _calculate_cost(self, usage: Dict[str, int]) -> float:
        """Calculate cost based on token usage"""
        # DeepSeek pricing (much cheaper than OpenAI)
        total_tokens = usage.get("total_tokens", 0)
        return total_tokens * 0.000001  # $0.001 per 1K tokens


class AIOrchestrator:
    """Orchestrates multiple AI providers with fallback logic"""

    def __init__(self, settings):
        self.settings = settings
        self.providers: Dict[str, AIProvider] = {}
        self.fallback_chain = ["openai", "deepseek"]
        self.health_check_interval = 60  # seconds
        self._health_check_task = None

    async def initialize(self):
        """Initialize all AI providers"""

        # Initialize OpenAI provider
        if AIProviderConfig.is_provider_available("openai", self.settings):
            openai_config = AIProviderConfig.get_provider_config("openai", self.settings)
            openai_provider = OpenAIProvider(openai_config)
            await openai_provider.initialize()
            self.providers["openai"] = openai_provider

        # Initialize DeepSeek provider
        if AIProviderConfig.is_provider_available("deepseek", self.settings):
            deepseek_config = AIProviderConfig.get_provider_config("deepseek", self.settings)
            deepseek_provider = DeepSeekProvider(deepseek_config)
            await deepseek_provider.initialize()
            self.providers["deepseek"] = deepseek_provider

        # Start health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())

        logger.info(
            "ai_orchestrator_initialized",
            providers=list(self.providers.keys())
        )

    async def cleanup(self):
        """Cleanup AI providers"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        for provider in self.providers.values():
            if hasattr(provider, 'client'):
                await provider.client.aclose()

        logger.info("ai_orchestrator_cleaned")

    def is_ready(self) -> bool:
        """Check if at least one provider is ready"""
        return any(provider.is_available for provider in self.providers.values())

    def get_available_providers(self) -> List[str]:
        """Get list of available providers"""
        return [
            name for name, provider in self.providers.items()
            if provider.is_available
        ]

    def get_provider_status(self) -> Dict[str, bool]:
        """Get status of all providers"""
        return {
            name: provider.is_available
            for name, provider in self.providers.items()
        }

    async def process_message(
        self,
        message: str,
        context: Dict[str, Any] = None,
        platform: str = None,
        language: str = None,
        preferred_provider: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process message with AI and return response with intent analysis"""

        # Determine provider order
        if preferred_provider and preferred_provider in self.get_available_providers():
            provider_order = [preferred_provider] + [p for p in self.fallback_chain if p != preferred_provider]
        else:
            provider_order = self.fallback_chain

        # Filter to only available providers
        provider_order = [p for p in provider_order if p in self.get_available_providers()]

        if not provider_order:
            raise Exception("No AI providers available")

        last_error = None

        for provider_name in provider_order:
            provider = self.providers[provider_name]

            try:
                logger.info(
                    "ai_processing_attempt",
                    provider=provider_name,
                    message_preview=message[:100]
                )

                # Concurrent intent analysis and response generation
                intent_task = provider.analyze_intent(message, context)
                response_task = provider.generate_response(message, context)

                intent_result, response_result = await asyncio.gather(
                    intent_task, response_task
                )

                # Combine results
                final_result = {
                    "response": response_result["response"],
                    "intent": intent_result["intent"],
                    "confidence": intent_result["confidence"],
                    "entities": intent_result["entities"],
                    "language": intent_result["language"],
                    "provider": provider_name,
                    "model": response_result["model"],
                    "usage": response_result.get("usage", {}),
                    "processing_time_ms": max(
                        response_result.get("processing_time_ms", 0),
                        intent_result.get("processing_time_ms", 0)
                    ),
                    "intent_processing": intent_result,
                    "response_processing": response_result
                }

                logger.info(
                    "ai_processing_success",
                    provider=provider_name,
                    intent=intent_result["intent"],
                    confidence=intent_result["confidence"]
                )

                return final_result

            except Exception as e:
                last_error = e
                logger.warning(
                    "ai_provider_failed",
                    provider=provider_name,
                    error=str(e),
                    remaining_providers=len(provider_order) - provider_order.index(provider_name) - 1
                )

                if provider_name != provider_order[-1]:
                    # Fallback to next provider
                    ai_logger.log_ai_fallback(
                        primary_provider=provider_name,
                        fallback_provider=provider_order[provider_order.index(provider_name) + 1],
                        reason=str(e)
                    )
                    continue
                else:
                    # Last provider failed
                    break

        # All providers failed
        logger.error(
            "all_ai_providers_failed",
            last_error=str(last_error),
            available_providers=self.get_available_providers()
        )

        raise Exception(f"All AI providers failed. Last error: {str(last_error)}")

    async def _health_check_loop(self):
        """Background health check loop"""
        while True:
            try:
                await self._check_all_providers_health()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "health_check_loop_error",
                    error=str(e),
                    exc_info=True
                )
                await asyncio.sleep(60)

    async def _check_all_providers_health(self):
        """Check health of all providers"""

        for provider_name, provider in self.providers.items():
            try:
                is_healthy = await provider.health_check()
                if is_healthy != provider.is_available:
                    provider.is_available = is_healthy
                    logger.info(
                        "provider_health_changed",
                        provider=provider_name,
                        is_healthy=is_healthy
                    )
            except Exception as e:
                logger.warning(
                    "provider_health_check_error",
                    provider=provider_name,
                    error=str(e)
                )
                provider.is_available = False
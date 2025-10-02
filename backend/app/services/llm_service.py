import aiohttp
import asyncio
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class LLMService:
    """Сервис для работы с LLM API (chutes)"""
    
    def __init__(self):
        self.api_token = os.getenv("CHUTES_API_TOKEN")
        self.base_url = "https://llm.chutes.ai/v1/chat/completions"
        
    async def ask(self, message: str) -> str:
        """Отправляет сообщение в LLM и получает ответ"""
        if not self.api_token:
            return "Ошибка: CHUTES_API_TOKEN не настроен в переменных окружения"
        
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

        body = {
            "model": "openai/gpt-oss-20b",
            "messages": [
                {"role": "user", "content": message},
            ],
            "stream": False,
            "max_tokens": 2048,
            "temperature": 0.7,
        }

        # Пробуем несколько раз с увеличивающимся таймаутом
        for attempt in range(3):
            try:
                timeout = aiohttp.ClientTimeout(total=30 + (attempt * 10))  # 30, 40, 50 секунд
                logger.info(f"LLM запрос, попытка {attempt + 1}/3, таймаут: {timeout.total}s")
                
                # Отключаем проверку SSL (использовать только если доверяете целевому хосту)
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False),
                    timeout=timeout
                ) as session:
                    async with session.post(
                        self.base_url,
                        headers=headers,
                        json=body,
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            content = data['choices'][0]['message']['content']
                            logger.info(f"LLM Response length: {len(content)} characters")
                            logger.info(f"LLM Response preview: {content[:200]}...")
                            return content
                        else:
                            error_text = await response.text()
                            logger.error(f"LLM API ошибка {response.status}: {error_text}")
                            if attempt == 2:  # Последняя попытка
                                return f"Извините, произошла ошибка {response.status} при обработке запроса."
                            else:
                                logger.info(f"Повторяем попытку через 2 секунды...")
                                await asyncio.sleep(2)
                                continue
            except asyncio.TimeoutError:
                logger.error(f"LLM API таймаут на попытке {attempt + 1}")
                if attempt == 2:  # Последняя попытка
                    return "Ошибка: Превышено время ожидания ответа от LLM API"
                else:
                    logger.info(f"Повторяем попытку через 2 секунды...")
                    await asyncio.sleep(2)
                    continue
            except Exception as e:
                logger.error(f"Ошибка при обращении к LLM API (попытка {attempt + 1}): {str(e)}")
                if attempt == 2:  # Последняя попытка
                    return f"Ошибка при обращении к LLM API: {str(e)}"
                else:
                    logger.info(f"Повторяем попытку через 2 секунды...")
                    await asyncio.sleep(2)
                    continue
        
        return "Ошибка: Не удалось получить ответ от LLM API после 3 попыток"


# Создаем глобальный экземпляр сервиса
llm_service = LLMService()

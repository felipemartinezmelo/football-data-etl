import os
import logging
import requests

from zoneinfo import ZoneInfo
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List
from src.utils.request_endpoint import RequestEndpoint

load_dotenv()

logger = logging.getLogger(__name__)

SUCCESS_COLOR = 0x03D100
ERROR_COLOR = 0xFF0000

SUCCESS_EMOJI = "✅"
ERROR_EMOJI = "❌"

TIMEZONE = ZoneInfo("America/Sao_Paulo")

class DiscordWebhookMessage:
    def __init__(self, webhook_url: Optional[str] = None) -> None:
        self.webhook_url: Optional[str] = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")

        if not self.webhook_url:
            raise ValueError(
                "Webhook URL não configurada. "
                "Defina DISCORD_WEBHOOK_URL no .env ou passe via construtor."
            )

    def send_message(
        self,
        title: str,
        message: str,
        success: bool,
        service_type: str,
        success_count: Optional[int] = None,
        error_count: Optional[int] = None,
    ) -> None:

        timestamp: datetime = datetime.now(TIMEZONE)

        payload: Dict[str, Any] = self._build_embed(
            title=title,
            message=message,
            timestamp=timestamp,
            success=success,
            service_type=service_type,
            success_count=success_count,
            error_count=error_count,
        )

        try:
            response: requests.Response = RequestEndpoint().request(self.webhook_url, payload,)
            response.raise_for_status()

            logger.info("Mensagem enviada com sucesso para o Discord (%s)", service_type,)

        except requests.exceptions.RequestException:
            logger.exception("Erro ao enviar mensagem para o Discord (%s)", service_type,)
            raise

    def _get_avatar(self, service_type: str) -> Optional[str]:
        env_var: str = f"AVATAR_{service_type.upper()}"
        avatar_url: Optional[str] = os.getenv(env_var)

        if not avatar_url:
            logger.warning(
                "Avatar não encontrado para o serviço '%s'. "
                "Verifique o .env.",
                service_type,
            )

        return avatar_url

    def _build_embed(
        self,
        title: str,
        message: str,
        timestamp: datetime,
        success: bool,
        service_type: str,
        success_count: Optional[int],
        error_count: Optional[int],
    ) -> Dict[str, Any]:

        color: int = SUCCESS_COLOR if success else ERROR_COLOR
        emoji: str = SUCCESS_EMOJI if success else ERROR_EMOJI
        avatar: Optional[str] = self._get_avatar(service_type)

        fields: List[Dict[str, Any]] = [
            {
                "name": "Project Name",
                "value": title,
                "inline": False,
            },
            {
                "name": "Time (America/Sao_Paulo)",
                "value": timestamp.strftime("%d/%m/%Y %H:%M:%S"),
                "inline": True,
            },
        ]

        if success_count is not None:
            fields.append(
                {
                    "name": "Success Count",
                    "value": str(success_count),
                    "inline": True,
                }
            )

        if error_count is not None:
            fields.append(
                {
                    "name": "Error Count",
                    "value": str(error_count),
                    "inline": True,
                }
            )

        payload: Dict[str, Any] = {
            "content": "📢 Hello @everyone",
            "username": "Automação ETL",
            "avatar_url": avatar,
            "embeds": [
                {
                    "title": f"{emoji} {title} {emoji}",
                    "description": message,
                    "color": color,
                    "fields": fields,
                }
            ],
        }

        return payload
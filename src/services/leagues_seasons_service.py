import logging

from typing import Any, Dict, List, Tuple
from src.utils.connections import Connections
from src.clients.football_api_client import FootballApiClient
from src.utils.discord_webhook_message import DiscordWebhookMessage

logger = logging.getLogger(__name__)

class LeagueSeasonsService:
    def execute(self) -> None:
        self.error = False

        try:
            with Connections("etl") as (conn, cursor):

                self.conn = conn
                self.cursor = cursor

                logger.info("Conexão com o banco ETL estabelecida")

                credentials = self.get_credentials()

                if not credentials:
                    logger.warning("Credenciais da API-Football não encontradas")
                    return

                client = FootballApiClient(base_url=credentials["url_api"], api_key=credentials["app_secret_key"])

                raw_data = self.extract(client)

                if not raw_data:
                    logger.warning("Nenhuma temporada retornado da API")
                    return

                mapped_data = self.transform(raw_data)

                if not mapped_data:
                    logger.warning("Nenhuma temporada válida após transformação")
                    return

                self.load(mapped_data)

        except Exception as e:
            self.error = True

            logger.error(f"Erro na execução do ETL das temporadas: {e}")

            DiscordWebhookMessage.send_message("Futebol - Temporadas", f"Erro durante a execução: {e}", False, "football")

        finally:
            logger.info("Processo de ETL das temporadas finalizado")

        if not self.error:
            DiscordWebhookMessage.send_message("Futebol - Temporadas", "Processo executado com sucesso.", True, "football")

    def get_credentials(self) -> Dict[str, str]:
        logger.info("Buscando credenciais da API-Football")

        sql = """
        SELECT app_secret_key, url_api
        FROM etl.credenciais
        WHERE nome_sistema = 'API-Football'
        LIMIT 1
        """

        self.cursor.execute(sql)

        result = self.cursor.fetchone()

        return result

    def extract(self, client: FootballApiClient) -> List[int]:
        logger.info("Extraindo temporadas da API")

        return client.get_seasons()

    def transform(self, data: List[int]) -> List[Tuple[int]]:
        logger.info("Transformando dados das temporadas")

        mapped = [(year,) for year in data]

        logger.info(f"{len(mapped)} temporadas preparadas para inserção")

        return mapped

    def load(self, data: List[Tuple[int]]) -> None:
        logger.info(f"Inserindo {len(data)} temporadas no banco de dados")

        sql = """
        INSERT INTO futebol_ligas_temporadas
        (
            temporada
        )
        VALUES (%s)
        """

        try:
            self.cursor.executemany(sql, data)

            self.conn.commit()

            logger.info("Inserção de temporadas concluída com sucesso")

        except Exception as e:
            self.conn.rollback()

            logger.error(f"Erro ao inserir temporadas no banco de dados: {e}")
            raise
import logging
from typing import Any, Dict, List, Tuple

from src.clients.football_api_client import FootballApiClient
from src.utils.connections import Connections
from src.utils.discord_webhook_message import DiscordWebhookMessage

logger = logging.getLogger(__name__)

class LeaguesService:
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

                client = FootballApiClient(base_url=credentials["urlApi"], api_key=credentials["appSecretKey"])

                countries_map = self.get_countries_db()

                raw_data = self.extract(client)
                if not raw_data:
                    logger.warning("Nenhuma liga retornada da API")
                    return

                mapped_data = self.transform(raw_data, countries_map)
                if not mapped_data:
                    logger.warning("Nenhuma liga válida após transformação")
                    return

                self.load(mapped_data)

        except Exception as e:
            self.error = True
            logger.error(f"Erro na execução do ETL das ligas: {e}")
            DiscordWebhookMessage.send_message("Futebol - Ligas", f"Erro durante a execução: {e}", False, "football")

        finally:
            logger.info("Processo de ETL das ligas finalizado")

        if not self.error:
            DiscordWebhookMessage.send_message("Futebol - Ligas", "Processo executado com sucesso.", True, "football")


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

    def get_countries_db(self) -> Dict[str, int]:
        logger.info("Buscando países já inseridos no banco de dados")
        sql = "SELECT id, nome FROM futebol_paises"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return {name.lower(): id for id, name in rows}

    def extract(self, client: FootballApiClient) -> List[Dict[str, Any]]:
        logger.info("Extraindo todas as ligas da API")
        return client.get_leagues()

    def transform(self, data: List[Dict[str, Any]], countries_map: Dict[str, int]) -> List[Tuple]:
        logger.info("Transformando dados das ligas")

        mapped = []

        for item in data:
            league = item.get("league", {})
            country = item.get("country", {})

            country_name = (country.get("name") or "").lower()
            country_id = countries_map.get(country_name)

            codigo = league.get("id")
            nome = league.get("name")
            tipo = league.get("type")
            url_logo = league.get("logo")

            mapped.append((codigo, nome, tipo, country_id, url_logo))

        logger.info(f"{len(mapped)} ligas preparadas para inserção")
        return mapped

    def load(self, data: List[Tuple]) -> None:
        logger.info(f"Inserindo {len(data)} ligas no banco de dados")

        sql = """
        INSERT INTO futebol_ligas
        (
            codigo,        
            nome,
            tipo,
            pais_id,
            url_logo
        )
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            nome = VALUES(nome),
            tipo = VALUES(tipo),
            url_logo = VALUES(url_logo),
            pais_id = VALUES(pais_id)
        """
        try:
            self.cursor.executemany(sql, data)
            self.conn.commit()
            logger.info("Inserção de ligas concluída com sucesso")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir ligas no banco de dados: {e}")
            raise
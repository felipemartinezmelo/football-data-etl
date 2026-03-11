import logging
import time
from typing import Any, Iterable, List, Optional, Tuple

import mysql.connector
from mysql.connector import MySQLConnection

logger = logging.getLogger(__name__)

class RequestControl:
    def __init__(
        self,
        connection: MySQLConnection,
        table_name: str = "controle_requisicao",
        max_retries: int = 5,
        retry_delay: float = 0.5,
    ) -> None:
        self.connection = connection
        self.cursor = connection.cursor(dictionary=True)
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        if not table_name.replace("_", "").isalnum():
            raise ValueError("Nome de tabela inválido.")

        self.table_name = table_name

    def __enter__(self) -> "RequestControl":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.connection.rollback()

        if self.cursor:
            self.cursor.close()

    def create(self, entity_ids: Iterable[str], request_type: str, start_page: int = 1,) -> None:
        values: List[Tuple[Any, ...]] = [(entity_id, request_type, start_page, 0) for entity_id in entity_ids]

        sql = f"""
            INSERT IGNORE INTO {self.table_name}
            (entidade_id, tipo_requisicao, pagina, completado)
            VALUES (%s, %s, %s, %s)
        """

        self._execute(sql, values)

    def get_page(self, entity_id: str, request_type: str, lock: bool = False,) -> int:
        sql = f"""
            SELECT pagina
            FROM {self.table_name}
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
        """

        if lock:
            sql += " FOR UPDATE"

        result = self._execute(sql, (entity_id, request_type), fetch=True,)

        if result:
            return result[0]["pagina"]

        return 1

    def is_completed(self, entity_id: str, request_type: str,) -> bool:
        sql = f"""
            SELECT 1
            FROM {self.table_name}
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
              AND completado = 0
        """

        result = self._execute(sql, (entity_id, request_type), fetch=True,)

        return len(result) == 0

    def update_page(self, entity_id: str, request_type: str, page: int,) -> None:
        sql = f"""
            UPDATE {self.table_name}
            SET pagina = %s
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
        """

        self._execute(sql, (page, entity_id, request_type),)

    def increment_page(self, entity_id: str, request_type: str,) -> None:
        sql = f"""
            UPDATE {self.table_name}
            SET pagina = pagina + 1
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
        """

        self._execute(sql, (entity_id, request_type),)

    def mark_completed(self, entity_id: str, request_type: str,) -> None:
        sql = f"""
            UPDATE {self.table_name}
            SET completado = 1
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
              AND completado = 0
        """

        self._execute(sql, (entity_id, request_type),)

    def delete_by_type(self, request_type: str,) -> None:
        sql = f"""
            DELETE FROM {self.table_name}
            WHERE tipo_requisicao = %s
        """

        self._execute(sql, (request_type,),)

    def delete_entity(self, entity_id: str, request_type: str,) -> None:
        sql = f"""
            DELETE FROM {self.table_name}
            WHERE entidade_id = %s
              AND tipo_requisicao = %s
        """

        self._execute(sql, (entity_id, request_type),)

    def _execute(self, sql: str, params: Optional[Any] = None, fetch: bool = False,) -> Optional[List[Any]]:
        attempt = 0

        while attempt < self.max_retries:
            try:
                if isinstance(params, list):
                    self.cursor.executemany(sql, params)

                else:
                    self.cursor.execute(sql, params)

                if fetch:
                    return self.cursor.fetchall()

                self.connection.commit()
                return None

            except mysql.connector.Error as e:
                self.connection.rollback()
                attempt += 1

                logger.warning("Erro SQL (tentativa %s/%s): %s", attempt, self.max_retries, e,)

                if attempt >= self.max_retries:
                    logger.error("Falha definitiva na execução SQL.")
                    raise

                time.sleep(self.retry_delay)

        return None
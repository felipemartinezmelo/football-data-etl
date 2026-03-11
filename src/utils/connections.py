import os
from typing import Any, Optional, Tuple, Union

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class Connections:
    def __init__(self, section: str) -> None:
        self.section: str = section.upper()
        self.conn: Optional[Any] = None
        self.cursor: Optional[Any] = None

    def __enter__(
        self,
    ) -> Union[
        Tuple[
            mysql.connector.connection.MySQLConnection,
            mysql.connector.cursor.MySQLCursor,
        ]
    ]:
        try:
            match self.section:
                case "ETL":
                    self.conn = mysql.connector.connect(
                        host=os.getenv(f"{self.section}_HOST"),
                        user=os.getenv(f"{self.section}_USER"),
                        password=os.getenv(f"{self.section}_PASSWORD"),
                        database=os.getenv(f"{self.section}_DATABASE"),
                        port=int(os.getenv(f"{self.section}_PORT")),
                        auth_plugin="mysql_native_password",
                        charset="utf8",
                    )

                    self.cursor = self.conn.cursor(dictionary=True)

                    return self.conn, self.cursor

                case _:
                    raise ValueError(f"Seção não suportada: {self.section}")

        except Exception as e:
            raise Exception(f"Erro ao conectar ao recurso {self.section}: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if self.cursor:
                self.cursor.close()

            if self.conn:
                self.conn.close()

        finally:
            if exc_type:
                print(f"Erro ocorrido: {exc_val}")
                print(f"Rastreamento da pilha: {exc_tb}")

        return False

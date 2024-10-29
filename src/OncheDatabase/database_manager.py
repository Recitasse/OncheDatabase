from getpass import getpass, getuser

from mysql.connector import MySQLConnection
from mysql.connector import connect as connect_server

from typing import Any
from OncheDatabase.utils.logger import MANAGER_LOG
from OncheDatabase._typing import MySQLResults
from dataclasses import dataclass, field


@dataclass(init=True)
class DatabaseManager:
    user: str = field(init=False, default="root")
    host: str = field(init=False, default="localhost")
    _password: str = field(init=False, default="")

    connexion: MySQLConnection = field(default=None, init=False)
    cursor: Any = None

    def __post_init__(self) -> None:
        """
        Init only for the connexion and the password
        :return:
        """
        MANAGER_LOG.info("\n\n===========================\n"
                         "     START NEW SESSION      \n"
                         "===========================\n\n")
        self._password = getpass("Give password to connect to mysql as root: ")
        try:
            self.connexion = connect_server(
                host=self.host,
                user='root',
                password=self._password,
                auth_plugin='mysql_native_password'
            )
            MANAGER_LOG.info(f"\n\n     °˖✧◝(⁰▿⁰)◜✧˖°     \n\n"
                             f"Connexion à mysql effectuée.\n"
                             f"SESSION HAS STARTED")
        except Exception as e:
            MANAGER_LOG.error(f"\n\n     (┛◉Д◉)┛彡┻━┻      \n\n"
                              f"Erreur. Impossible de se connecter à mysql.\n"
                              f"\n{e}"
                              f"\nCOULD NOT START SESSION")
            raise Exception(e)

    def query(self, query: str, values: tuple = None) -> MySQLResults:
        """
        Execute la query demandée avec la valeur demandée
        :param query: query à exécuter
        :param values: paramètre de la query en tuple
        :return: Résultats SQL
        """
        results_ = []
        with self as cursor:
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query, params=values)
        res = cursor.fetchall()
        if res and res[0]:
            results_ = [
                k.decode('utf-8') if isinstance(k, (bytearray, bytes)) else k
                for k in res[0]
            ]
        return results_


    def get_all_database(self) -> MySQLResults:
        """
        Return all database result
        :return: query results
        """
        return self.query("SHOW DATABASES;")

    def __enter__(self):
        """
        Créé le curseur pour requêtes MySQL
        :return: Cursor
        """
        self.cursor = None
        try:
            self.cursor = self.connexion.cursor()
        except Exception as e:
            MANAGER_LOG.error(f"Erreur : {e}")
        finally:
            if self.cursor is None:
                raise ValueError(
                    f"Le curseur MySQL n'a pas été créé correctement."
                )
            return self.cursor

    def __exit__(self):
        """
        Ferme le curseur MySQL
        :return: None
        """
        try:
            if self.connexion:
                self.connexion.commit()
            if self.cursor:
                self.cursor.close()
            self.cursor = None
        except Exception as e:
            MANAGER_LOG.error(f"Erreur : {e}")
        finally:
            if self.cursor is not None:
                raise ValueError(
                    f"Le curseur MySQL n'a pas été fermé correctement"
                )

ddb = DatabaseManager()
print(ddb.get_all_database())

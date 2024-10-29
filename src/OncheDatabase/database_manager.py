from OncheDatabase.link import Link, Query
from OncheDatabase.utils.logger import MANAGER_LOG
from OncheDatabase._typing import MySQLResults
from dataclasses import dataclass


@dataclass
class DatabaseManager(Link):
    def available_databases(self) -> MySQLResults:
        """
        Renvoie toutes les bases de données actives
        :return:
        """
        return self.query(Query.DATABASES)

    def create_database(self, name: str) -> None:
        """
        Créer une base de donnée
        :param name: nom de la base de donnée
        :return: None
        """
        self.query(f"CREATE DATABASE {name};")
        if name not in self.available_databases():
            MANAGER_LOG.error(
                f"La création de la base de donnée {name} a échoué."
            )
            raise Exception(
                f"La création de la base de donnée {name} a échoué."
            )
        MANAGER_LOG.info(f"Création de la base de donnée {name} effectuée.")

    def remove_database(self, name: str) -> None:
        """
        Efface la base de donnée donnée
        :param name: nom de la base de donnée
        :return: None
        """
        self.query(f"DROP DATABASE {name};")
        if name not in self.available_databases():
            MANAGER_LOG.error(
                f"La suppression de la base de donnée {name} a échoué."
            )
            raise Exception(
                f"La suppression de la base de donnée {name} a échoué."
            )
        MANAGER_LOG.warn(f"suppression de la base de donnée {name} effectuée.")

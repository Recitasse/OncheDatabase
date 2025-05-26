from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional, Union, Sequence
from src.OncheDatabase._typing import MySQLResults
from src.OncheDatabase.tools.MySQLAbc import MysqlAbc
from src.OncheDatabase.utils.logger import QUERY_LOG, MANAGER_LOG
from typing import Tuple, ClassVar


@dataclass(init=True)
class DatabaseManager(MysqlAbc):
    """
    Objet de la database, permettant toutes les commandes sur MySQL directement
    La création de base de donnée, le changement de privilèges etc
    """

    UNTOUCHABLE: ClassVar[Tuple[str]] = (
        'information_schema', 'mysql', 'performance_schema', 'sys'
    )

    def __post_init__(self):
        super().__post_init__()

    def delete(self, name: Optional[str] = None,
                        startswith: Optional[str] = None,
                        endswith: Optional[str] = None,
                        contains: Optional[str] = None) -> None:
        """
        Supprime la base de donnée sélectionnée
        :param name: name
        :param startswith: patern de début
        :param endswith: patern de fin
        :param contains: patern interne
        :return: Résultats de la query
        """
        if name in DatabaseManager.UNTOUCHABLE:
            MANAGER_LOG.error(f"Impossible de supprimer {name}.")
            return
        if name:
            if name not in self.get_database():
                MANAGER_LOG.info(f"La base de donnée {name} n'existe pas.")
                return
            query = f"DROP DATABASE {name};"
            MANAGER_LOG.warning(f"Base de donnée {name} supprimée.")
        elif startswith or endswith or contains:
            query = (f"DROP DATABASES LIKE "
                     f"{self.pattern(startswith, endswith, contains)};")
            QUERY_LOG.info(query)
            all_database = self.get_database(f"SHOW DATABASES LIKE "
                     f"{self.pattern(startswith, endswith, contains)};")
            MANAGER_LOG.warning(f"Suppression des bases de donnée suivantes:\n"
                                f"{'    \n.'.join([n for n in all_database])}")
        else:
            raise ValueError(f"Aucun paramètre / nom donnés.")
        self.query(query)

    def create(self, name: str) -> None:
        """
        Créer un base de donnée de nom donné
        :param name: nom de la base de donnée
        :return:
        """
        err_name = ["information_schema", "mysql",
                    "performance_schema", "sys"]
        if name in err_name:
            raise ValueError(f"Vous ne pouvez pas choisir le nom {name}")

        self.query(f"CREATE DATABASE IF NOT EXISTS {name};")
        MANAGER_LOG.info(f"Création de la base de donnée {name}.")
        self.grant_privilege_to_database(name)

    def modify(self, *args, **kwargs) -> None:
        ...

    def grant_privilege_to_database(self,
                                    database: Union[
                                        str,
                                        Iterable[str]
                                    ],
                                    user: Union[
                                        str,
                                        Iterable[str]
                                    ] = "root") -> None:
        """
        Octroie les privilèges à un utilisateur donné sur une base de donnée
        donnée
        :param database: Nom de la base de donnée
        :param user: Nom de l'utilisateur (default root)
        :return: None
        """

        if isinstance(user, str):
            user = [user]
        if isinstance(database, str):
            database = [database]

        for us in user:
            for ddb in database:
                self.query(f"GRANT ALL PRIVILEGES ON {ddb}.* TO "
                           f"'{us}'@'localhost' WITH GRANT OPTION;")
                MANAGER_LOG.info(
                    f"Privilèges donnés à {us} pour {ddb}.")
        self.query("FLUSH PRIVILEGES;")

    def get_database(self, startswith: Optional[str] = None,
                         endswith: Optional[str] = None,
                         contains: Optional[str] = None) -> MySQLResults:
        """
        Récupère toutes les bases de données avec des patern donnés
        :param startswith: patern de début
        :param endswith: patern de fin
        :param contains: patern interne
        :return: Résultats de la query
        """
        if not startswith or not endswith or not contains:
            query = f"SHOW DATABASES;"
        else:
            query = (f"SHOW DATABASES LIKE "
                     f"{self.pattern(startswith, endswith, contains)};")
        QUERY_LOG.info(query)
        return self.query(query)

    @staticmethod
    def pattern(startswith: Optional[str] = None,
                endswith: Optional[str] = None,
                contains: Optional[str] = None):
        pattern = ''
        if startswith:
            pattern += f'{startswith}%'
        if contains:
            pattern += f'{contains}%'
        if endswith:
            pattern += f'{endswith}'
        return f"'{pattern}'"

    @property
    def databases(self) -> Sequence[str]:
        """
        Renvoie toutes les bases de données
        :return: Tuple des bases de données existantes
        """
        return self.get_database()

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional, Union, Sequence
from OncheDatabase._typing import MySQLResults
from OncheDatabase.tools.MySQLConnexion import MySQLConnexion, QUERY_LOG, MANAGER_LOG
from typing import Tuple, ClassVar


@dataclass(init=True)
class DatabaseManager(MySQLConnexion):
    UNTOUCHABLE: ClassVar[Tuple[str]] = (
        'information_schema', 'mysql', 'performance_schema', 'sys'
    )
    def __post_init__(self):
        super().__post_init__()

    def get_all_database(self, startswith: Optional[str] = None,
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

    def show_users(self) -> MySQLResults:
        """
        Montre tous les utilisateurs Mysql
        :return: les utilisateurs
        """
        query = "SELECT User, Host FROM mysql.user;"
        QUERY_LOG.info(query)
        return self.query(query)

    def change_users(self, user: str, password: str,
                        host: str = "localhost",
                        database: Optional[str] = None) -> None:
        """
        Change l'utilisateur de la base de donnée
        :param user: nom de l'utilisateur
        :param password: mdp de la bdd
        :param host: host de la bdd
        :param database: nom de la base de donnée
        """
        MANAGER_LOG.warning(f"Changement d'utilisateur : {user}")
        self._setup_connexion(
            user=user, host=host, password=password, database=database
        )

    def delete_users(self, user: str, host: str = "localhost") -> None:
        """
        Efface l'utilisateur donnée
        :param user: nom de l'utilisateur
        :param host: hostname de l'utilisateur
        :return:
        """
        if user == "root":
            MANAGER_LOG.error(f"Impossible de supprimer l'utilisateur root.")
            return

        query = f"DROP USER IF EXISTS '{user}'@'{host}';"
        self.query(query)
        MANAGER_LOG.warning(f"Utilisateur {user} a été effacé.")

    def delete_database(self, name: Optional[str] = None,
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
            if name not in self.get_all_database():
                MANAGER_LOG.info(f"La base de donnée {name} n'existe pas.")
                return
            query = f"DROP DATABASE {name};"
            MANAGER_LOG.warning(f"Base de donnée {name} supprimée.")
        elif startswith or endswith or contains:
            query = (f"DROP DATABASES LIKE "
                     f"{self.pattern(startswith, endswith, contains)};")
            QUERY_LOG.info(query)
            all_database = self.get_all_database(f"SHOW DATABASES LIKE "
                     f"{self.pattern(startswith, endswith, contains)};")
            MANAGER_LOG.warning(f"Suppression des bases de donnée suivantes:\n"
                                f"{'    \n.'.join([n for n in all_database])}")
        else:
            raise ValueError(f"Aucun paramètre / nom donnés.")
        self.query(query)

    def create_database(self, name: str) -> None:
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

    def show_privileges(self,
                        name: Union[str, Iterable[str]] = "root") -> str:
        """
        Montre les privilèges d'une base de donnée
        :param name: Nom de la base de donnée
        :return: None
        """
        if isinstance(name, str):
            name = [name]

        for nm in name:
            query = (f"SELECT * FROM information_schema.SCHEMA_PRIVILEGES "
                     f"WHERE TABLE_SCHEMA = '{nm}'")
            QUERY_LOG.info(f"Demande de visibilité sur les privilèges "
                           f"demandé sur {nm}.")
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

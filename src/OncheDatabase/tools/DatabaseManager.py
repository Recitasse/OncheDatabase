from collections.abc import Iterable
from dataclasses import dataclass
from functools import singledispatchmethod
from typing import Optional, Union, Sequence, Mapping
from OncheDatabase._typing import MySQLResults
from OncheDatabase.elements.Database import Database
from OncheDatabase.tools.MySQLConnexion import (MySQLConnexion,
                                                QUERY_LOG, MANAGER_LOG)
from OncheDatabase.utils.privileges import Privileges
from OncheDatabase.utils.errors import PrivilegeNotFound, PrivilegeErreur
from typing import Tuple, ClassVar

__all__ = ['DatabaseManager']

@dataclass(init=True)
class DatabaseManager(MySQLConnexion):
    UNTOUCHABLE: ClassVar[Tuple[str]] = (
        'information_schema', 'mysql', 'performance_schema', 'sys'
    )
    def __post_init__(self):
        super().__post_init__()

    def get_all_database(self, startswith: Optional[str] = None,
                         endswith: Optional[str] = None,
                         contains: Optional[str] = "Onch") -> MySQLResults:
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

    def get_all_users(self, startswith: Optional[str] = None,
                         endswith: Optional[str] = None,
                         contains: Optional[str] = "Onch") -> MySQLResults:
        """
        Récupère tous les noms d'utilisateur
        :param startswith: patern de début
        :param endswith: patern de fin
        :param contains: patern interne
        :return: Résultats de la query
        """
        if not startswith or not endswith or not contains:
            query = f"SELECT user FROM mysql.user;"
        else:
            query = (f"SELECT user FROM mysql.user LIKE"
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

    def create_user(self, user: str, host: str = "localhost") -> None:
        """
        Créer un utilisateur dans la base de donnée
        :param user: Nom utilisateur
        :param host: hostname
        :return: None
        """

    def add_right_user(self, user: str,
                          droits: Sequence[str] | str,
                          databases: Sequence[str] | str,
                          localhost: str = "localhost") -> None:
        """
        Ajoute des droits d'un utilisateur sur les bases de données
        Attention les les droits s'appliquent identiquement à toutes les bases
        de donnée citées
        :param user: utilisateur qui récupère les droits
        :param droits: les droits en question
        :param localhost: Localhost
        :param databases: les databases en questions
        :return: None
        """

        if not isinstance(droits, Sequence):
            droits = [droits]
        for droit in droits:
            if droit not in Privileges.__members__.values():
                raise PrivilegeNotFound(droit)
            if Privileges.GRANT in droits:
                raise PrivilegeErreur(droit)

        for database in databases:
            query = (f"GRANT {', '.join(droits)} ON {database}.* "
                     f"TO '{user}'@'{localhost}'")
            QUERY_LOG.info(query)
            MANAGER_LOG.info(query)
            self.query(query)
            self.query("FLUSH PRIVILEGES;")

    def revoke_right_user(self, user: str,
                          droits: Sequence[str] | str,
                          databases: Sequence[str] | str,
                          localhost: str = "localhost") -> None:
        """
        Retire des droits d'un utilisateur sur les bases de données
        Attention les les droits s'appliquent identiquement à toutes les bases
        de donnée citées
        :param user: utilisateur qui perd les droits
        :param droits: les droits en question
        :param localhost: Localhost
        :param databases: les databases en questions
        :return: None
        """

        if not isinstance(droits, Sequence):
            droits = [droits]
        for droit in droits:
            if droit not in Privileges.__members__.values():
                raise PrivilegeNotFound(droit)
            if Privileges.GRANT in droits:
                raise PrivilegeErreur(droit)

        for database in databases:
            query = (f"REVOKE {', '.join(droits)} ON {database}.* "
                     f"TO '{user}'@'{localhost}'")
            QUERY_LOG.info(query)
            MANAGER_LOG.info(query)
            self.query(query)
            self.query("FLUSH PRIVILEGES;")

    def show_privileges(self, users: Sequence[str] | str,
                        localhost: str = "localhost") -> Mapping[str, str]:
        """
        Montre les privilèges des utilisateurs
        :param users: les utilisateurs selectionnés
        :param localhost: localhost du serveur
        :return: liste des privilèges des utilsateurs
        """
        user_dict = {}
        for user in users:
            query = "SHOW GRANTS FOR %s@%s;"
            QUERY_LOG.info(query)
            MANAGER_LOG.info(query)
            privileges = self.get_results(
                query, params=(user, localhost), ind_="all"
            )
            user_dict.update({user: privileges})
        return user_dict


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
            if name not in self:
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
        if database not in self:
            MANAGER_LOG.warning(f"La base de donnée {database} n'existe pas.")
            return

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

    @singledispatchmethod
    def __contains__(self, item) -> bool:
        return NotImplemented(f"{type(item)} is not supported.")

    @__contains__.register(str)
    def _(self, item: str) -> bool:
        return item in self.get_all_database()

    @__contains__.register(Database)
    def _(self, item: Database) -> bool:
        return item.database_name in self.get_all_database()

    def __iter__(self) -> Iterable:
        return self.get_all_database().__iter__()

    def __bool__(self) -> bool:
        """
        :return: Return true if DatabaseManager is active
        """
        return True if self.connexion else False

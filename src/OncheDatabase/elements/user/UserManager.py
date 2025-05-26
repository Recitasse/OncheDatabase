from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional, Sequence
from src.OncheDatabase.tools.MySQLAbc import MysqlAbc
from src.OncheDatabase.utils.logger import QUERY_LOG, MANAGER_LOG
from typing import Tuple, ClassVar


@dataclass(init=True)
class UserManager(MysqlAbc):
    """
    Objet de la database, permettant toutes les commandes sur les utilisateurs MySQL directement
    La création de base de donnée, le changement de privilèges etc
    """

    UNTOUCHABLE: ClassVar[Tuple[str]] = (
        'root', 'mysql', 'debian'
    )

    def create(self, name: str, password: str, host: str = "localhost") -> None:
        """
        Créer un utilisateur
        :param name: nom de l'utilisateur
        :param password: mot de passe de l'utilisateur
        :param host: host pour l'utilisateur
        :return:
        """
        err_name = ["root", "mysql"]
        if name in err_name:
            raise ValueError(f"Vous ne pouvez pas choisir le nom '{name}'.")

        self.query(f"CREATE DATABASE IF NOT EXISTS `{name}`;")
        MANAGER_LOG.info(f"Création de la base de données '{name}'.")

        self.query(f"CREATE USER IF NOT EXISTS '{name}'@'localhost' IDENTIFIED BY '{password}';")
        self.query(f"GRANT ALL PRIVILEGES ON `{name}`.* TO '{name}'@'localhost';")
        self.query("FLUSH PRIVILEGES;")
        MANAGER_LOG.info(f"Création de l'utilisateur '{name}' avec accès complet à la base '{name}'.")


    def delete(self, user: str, host: str = "localhost") -> None:
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

    def modify(self, user: str,
               new_name: Optional[str] = None,
               new_password: Optional[str] = None,
               database: Optional[Iterable[str] | str] = None,
               privileges: Optional[Iterable[str] | str] = None
               ) -> None:
        """
        Modifie un utilisateur suivant les paramètres donnés
        :param user: l'utilisateur que l'on veut modifier
        :param new_name: Le nouveau nom de l'utilisateurs
        :param new_password: Le nouveau mot de passe
        :param database: les bases de données sur lequelles on veut changer de privilèges
        :param privileges: les nouveau privilèges de l'utilisateur
        :return: None
        """
        if user in UserManager.UNTOUCHABLE:
            raise ValueError(
                f"L'utilisateur '{user}' ne peut pas être modifié.")

        if new_name:
            self.query(
                f"RENAME USER '{user}'@'localhost' TO '{new_name}'@'localhost';")
            MANAGER_LOG.info(
                f"Utilisateur renommé de '{user}' à '{new_name}'.")
            user = new_name

        if new_password:
            self.query(
                f"ALTER USER '{user}'@'localhost' IDENTIFIED BY '{new_password}';")
            MANAGER_LOG.info(
                f"Mot de passe mis à jour pour l'utilisateur '{user}'.")

        if database:
            if isinstance(database, str):
                database = list(database)
            if isinstance(privileges, str):
                privileges = list(privileges)

            for db in database:
                self.query(
                    f"GRANT {privileges} PRIVILEGES ON `{db}`.* TO '{user}'@'localhost';")
                self.query("FLUSH PRIVILEGES;")
                MANAGER_LOG.info(
                    f"Privilèges '{privileges}' accordés à '{user}' sur la base '{db}'.")

    @property
    def users(self) -> Sequence[str]:
        """
        Montre tous les utilisateurs Mysql
        :return: les utilisateurs
        """
        query = "SELECT User, Host FROM mysql.user;"
        users = self.query(query)
        for user in UserManager.UNTOUCHABLE:
            if user in users:
                users.remove(user)
        QUERY_LOG.info(query)
        return users
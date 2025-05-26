from dataclasses import dataclass
from src.OncheDatabase.tools.MySQLConnexion import MySQLConnexion
from abc import ABC, abstractmethod


@dataclass(init=True)
class MysqlAbc(MySQLConnexion, ABC):
    """
    Objet de la database, permettant toutes les commandes sur les utilisateurs MySQL directement
    La création de base de donnée, le changement de privilèges etc
    """

    @abstractmethod
    def create(self, *args, **kwargs) -> None:
        """
        Créé un élément dans la base de donnée
        :param args: args de la fonction créée
        :param kwargs: kwargs de la fonction créée
        :return: None
        """
        ...

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """
        Efface un élément dans la base de donnée
        :param args: args de la fonction effacée
        :param kwargs: kwargs de la fonction effacée
        :return: None
        """
        ...

    @abstractmethod
    def modify(self, *args, **kwargs) -> None:
        """
        Modifie un élément dans la base de donnée
        :param args: args de la fonction modifiée
        :param kwargs: kwargs de la fonction modifiée
        :return: None
        """
        ...
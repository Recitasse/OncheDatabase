from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from inspect import currentframe
from os.path import join as join_path
from subprocess import run as run_subprocess
from typing import Any, Union, final

from mysql.connector import MySQLConnection
from mysql.connector import connect as connect_server

from OncheDatabase._typing import MySQLResults
from OncheDatabase.utils.logger import QUERY_LOG


__all__ = ['Query', 'Link']


class Query(StrEnum):
    SIZE = ("SELECT SUM(data_length + index_length) / 1024 / 1024 'Database "
            "Size in MB' FROM information_schema.tables "
            "WHERE table_schema = %s;")
    DATABASES = "SHOW DATABASES;"


class BddPath(StrEnum):
    OUTPUTS = r"data/outputs/"
    INPUTS = r"data/inputs/"
    REPORTS = r"data/reports/"


@dataclass(init=True)
class Link:
    user: str = field(default="Onche", init=True)
    host: str = field(default="localhost", init=True)
    database: str = field(default="Onche", init=True)
    _password: str = field(default="", init=True)

    connexion: MySQLConnection = None
    cursor: Any = None

    _verbose: bool = field(default=False, init=True)

    def __post_init__(self) -> None:
        """
        Init la connexion et le logger
        :return: None
        """
        self.connexion = connect_server(
            user=self.user, host=self.host,
            password=self._password,
            database=self.database,
            auth_plugin="mysql_native_password"
        )
        QUERY_LOG.info("Connexion à la base de donnée réussie.")

    def exporter_bdd(self, as_name: str = "bdd") -> None:
        """
        Exporte la base de donnée actuelle
        :param as_name: Le nom de la base de donnée
        :return: None
        """
        date = datetime.now().strftime("%d_%m_%Y-%H-%M-%S")
        file_name = join_path(BddPath.OUTPUTS, f'{as_name}_{date}.sql')
        try:
            run_subprocess(
                f"mysqldump -u{self.user} -p{self._password} "
                f"{self.database} > {file_name}",
                shell=True,
                check=True
            )
            QUERY_LOG.info(
                f"Base de donnée {self.database} exportée : "
                f"{file_name}"
            )
        except Exception as e:
            QUERY_LOG.error(
                f"Impossible d'exporter la BDD {self.database} : {e}"
            )
            raise Exception(
                f"Impossible d'exporter la BDD {self.database} : {e}"
            )

    @final
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

    @final
    def get_results(self, query: str, params: tuple = None,
                    ind_: Union[int, str] = 0) -> MySQLResults:
        """
        Récupère les résultats des query MySQL avec
        les paramètres voulu et les indices voulue
        :param query: La query à exécuter
        :param params: Paramètres Tuple
        :param ind_: L'indice de la séquence voulu (all pour tout récupérer)
        :return: Resultas SQL
        """
        with self as cursor:
            if params:
                cursor.execute(query, params=params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()

        if isinstance(ind_, str):
            vals = []
            if ind_ == "all":
                for value in results:
                    if len(value) > 1:
                        cco_name = currentframe().f_back.f_code.co_name
                        if cco_name == "get_table_info":
                            vals.append(list(value))
                        else:
                            tmp_ = [val for val in value]
                            vals.append(tmp_)
                    else:
                        vals = [value[0] for value in results]
                return tuple(vals)
            else:
                QUERY_LOG.warning(
                    "La l'indice demandé est erroné, indice 0 par défaut."
                )
                ind_ = 0

        if isinstance(ind_, int):
            if ind_ == 0:
                if results:
                    return results[0][0]
                return []
            elif ind_ > 0:
                vals = [value[0] for value in results]
                return vals[0][ind_]
        else:
            QUERY_LOG.error(
                "Impossible de demander un indice autre qu'un "
                "entier ou exceptionnellement 'all'."
            )
            raise ValueError(
                f"Impossible de récupérer l'indice {ind_} "
                f"du résultat de la requête select."
            )

    @property
    def size(self) -> MySQLResults:
        """
        Renvoie la taille de la base de donnée
        :return: la taille en numérique
        """
        try:
            return self.get_results(Query.SIZE, params=(self.database,))
        except Exception as e:
            QUERY_LOG.error(f"Une erreur est survenue : {e}.")
        return 0

    @property
    def bdd(self) -> dict:
        """
        Renvoie le dictionnaire de la connexion
        :return: user, mdp, bdd, host
        """
        return {
            'user': self.user,
            'mdp': self._password,
            'bdd': self.database,
            'host': self.host
        }

    @bdd.setter
    def bdd(self, database: str) -> None:
        """
        Change la base de donnée
        :param database: nom de la base de donnée
        :return: None
        """
        try:
            self.connexion = connect_server(
                user=self.user,
                host=self.host,
                password=self._password,
                database=database,
                auth_plugin="mysql_native_password"
            )
            QUERY_LOG.info(
                f"Connexion à la base de donnée {database} réussie."
            )
            self.database = database
        except Exception as e:
            QUERY_LOG.error(
                f"Echec de la connexion à la base de donnée {database} : {e}"
            )
            raise Exception(
                f"Echec de la connexion à la base de donnée {database} : {e}"
            )

    def __enter__(self):
        """
        Créé le curseur pour requêtes MySQL
        :return: Cursor
        """
        self.cursor = None
        try:
            self.cursor = self.connexion.cursor()
        except Exception as e:
            QUERY_LOG.error(f"Erreur : {e}")
        finally:
            if self.cursor is None:
                raise ValueError(
                    f"Le curseur MySQL n'a pas été créé correctement."
                )
            return self.cursor

    def __exit__(self) -> bool:
        """
        Ferme le curseur MySQL
        :return: False
        """
        try:
            if self.connexion:
                self.connexion.commit()
            if self.cursor:
                self.cursor.close()
            self.cursor = None
        except Exception as e:
            QUERY_LOG.error(f"Erreur : {e}")
        finally:
            if self.cursor is not None:
                raise ValueError(
                    f"Le curseur MySQL n'a pas été fermé correctement"
                )
            return False
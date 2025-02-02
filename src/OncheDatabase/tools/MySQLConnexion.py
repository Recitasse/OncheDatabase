from dataclasses import dataclass, field
from logging import getLogger
from inspect import currentframe
from typing import Any, Optional, Tuple, Union, final

from mysql.connector import MySQLConnection as Connexion
from mysql.connector import connect as connect_server

from OncheDatabase._typing import MySQLResults

QUERY_LOG = getLogger("QUERY")
MANAGER_LOG = getLogger("MANAGER")


@dataclass(init=True, order=False, repr=True)
class MySQLConnexion:
    user: str = field(
        init=False, default="root", hash=True, repr=True,
        metadata={"description": "user name"}
    )
    host: str = field(
        init=False, default="localhost", hash=True, repr=True,
        metadata={"description": "localhost name"}
    )
    database: Optional[str] = field(
        init=False, default=None, hash=True, repr=True,
        metadata={"description": "Database name"}
    )
    _password: str = field(
        init=False, hash=True, repr=True,
        metadata={"description": "password of mysql"}
    )

    connexion: Connexion = field(default=None, init=False)
    cursor: Any = None

    def __post_init__(self) -> None:
        """
        Init only for the connexion and the password
        :return:
        """
        MANAGER_LOG.info("\n\n===========================\n"
                         "     START NEW SESSION      \n"
                         "===========================\n\n")
        #self._password = getpass("Give password to connect to mysql as root: ")
        self._password = "C98ar5l2a#"
        self._setup_connexion(
            self.user, self.host, self._password, self.database
        )

    def _setup_connexion(self, user: Optional[str] = None,
                         host: Optional[str] = None,
                         password: Optional[str] = None,
                         database: Optional[str] = None) -> None:
        """
        Met en place la connexion avec les paramètres souhaités
        :param user: Utilisateur de la base de donnée
        :param host: Host de la base de donnée
        :param password: mdp de l'utilisateur
        :param database: nom de la base de donnée
        :return: None
        """
        self.user = user if user else self.user
        self.host = host if host else self.host
        self._password = password if password else self._password
        try:
            self.connexion = connect_server(
                host=host,
                user=user,
                password=password,
                auth_plugin='mysql_native_password'
            )
            MANAGER_LOG.info(f"\n\n     °˖✧◝(⁰▿⁰)◜✧˖°     \n\n"
                             f"Connexion à mysql effectuée.\n"
                             f"SESSION HAS STARTED AS {user}")
            if database:
                self.database = database
                self.connexion.database = database
        except Exception as e:
            MANAGER_LOG.error(f"\n\n     (┛◉Д◉)┛彡┻━┻      \n\n"
                              f"Impossible de se connecter à mysql.\n"
                              f"\n{e}"
                              f"\nCOULD NOT START SESSION AS {user}")
            raise Exception(e)

    @final
    def query(self, query: str,
              values: Optional[Tuple[Any, ...]] = None) -> MySQLResults:
        """
        Execute the query with the provided values.
        :param query: The SQL query to execute
        :param values: Parameters for the query as a tuple
        :return: SQL results as a list
        """
        results_ = []
        with self as cursor:
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)

            if cursor.with_rows:
                res = cursor.fetchall()
                results_ = [
                    k[0].decode('utf-8') if isinstance(
                        k[0],(bytearray, bytes)
                    ) else k[0]
                    for k in res
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

    def __enter__(self):
        """
        Create a MySQL cursor for executing queries.
        :return: Cursor
        """
        try:
            self.cursor = self.connexion.cursor()
            return self.cursor
        except Exception as e:
            MANAGER_LOG.error(f"{e}")
            raise ValueError("Le curseur n'a pas été créé correctement.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the MySQL cursor and handle exceptions.
        :return: None
        """
        try:
            if self.connexion:
                self.connexion.commit()
            if self.cursor:
                self.cursor.close()
                self.cursor = None
        except Exception as e:
            MANAGER_LOG.error(f"Impossible de fermer le curseur: {e}")
        finally:
            # Final cleanup check for the cursor
            if self.cursor is not None:
                raise ValueError("MySQL cursor was not closed properly.")

__all__ = ["MySQLConnexion", "MANAGER_LOG", "QUERY_LOG"]

from dataclasses import dataclass, field
from getpass import getpass
from src.OncheDatabase.utils.logger import QUERY_LOG, MANAGER_LOG
from inspect import currentframe
from typing import Any, Optional, Tuple, Union, final

from mysql.connector import MySQLConnection
from mysql.connector import connect

from src.OncheDatabase._typing import MySQLResults


@dataclass(init=True)
class MySQLConnexion:
    """
    Objet de la connexion MySQL avec les différentes configurations données
    :param user: Nom de l'utilisateur se connectant lors de la session MySQL
    :param host:
    :param database:
    :param _password:
    """
    user: str = field(init=False, default="root")
    host: str = field(init=False, default="localhost")
    database: Optional[str] = field(init=True, default=None)
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
        self._password = ""
        self._setup_connexion(
            self.user, self.host, self._password
        )

    def _setup_connexion(self, user: Optional[str] = None,
                         host: Optional[str] = None,
                         password: Optional[str] = None) -> None:
        """
        Met en place la connexion avec les paramètres souhaités
        :param user: Utilisateur de la base de donnée
        :param host: Host de la base de donnée
        :param password: mdp de l'utilisateur
        :return: None
        """
        self.user = user if user else self.user
        self.host = host if host else self.host
        self._password = password if password else self._password
        try:
            self.connexion = connect(
                host=host,
                user=user,
                password=password,
                auth_plugin='mysql_native_password'
            )
            MANAGER_LOG.info(f"\n\n     °˖✧◝(⁰▿⁰)◜✧˖°     \n\n"
                             f"Connexion à mysql effectuée.\n"
                             f"SESSION HAS STARTED AS {user}")
        except Exception as e:
            MANAGER_LOG.error(f"\n\n     (┛◉Д◉)┛彡┻━┻      \n\n"
                              f"Impossible de se connecter à mysql.\n"
                              f"\n{e}"
                              f"\nCOULD NOT START SESSION AS {user}")
            raise Exception(e)

    @final
    def change_users(self, user: str, password: str,
                        host: str = "localhost") -> None:
        """
        Change l'utilisateur de la base de donnée
        :param user: nom de l'utilisateur
        :param password: mdp de la bdd
        :param host: host de la bdd
        """
        MANAGER_LOG.warning(f"Changement d'utilisateur : {user}")
        self._setup_connexion(
            user=user, host=host, password=password
        )

    @final
    def query(self, query: str,
              values: Optional[Tuple[Any]] = None) -> MySQLResults:
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
    def call_routine(self, routine: str, *args) -> Optional[tuple]:
        """
        Execute une routine donnée
        :param routine: nom de la routine à executer
        :return:
        """
        with self as cursor:
             res = cursor.callproc(routine, args)
        return res

    @final
    def get_results(self, query: str, params: tuple = None,
                    g_index: Union[int, str] = 0) -> MySQLResults | None:
        """
        Récupère les résultats des query MySQL avec
        les paramètres voulu et les indices voulue
        :param query: La query à exécuter
        :param params: Paramètres Tuple
        :param g_index: L'indice de la séquence voulu (all pour tout récupérer)
        :return: Resultas SQL
        """
        with self as cursor:
            if params:
                cursor.execute(query, params=params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()

        if isinstance(g_index, str):
            vals = []
            if g_index == "all":
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
                g_index = 0

        if isinstance(g_index, int):
            if g_index == 0:
                if results:
                    return results[0][0]
                return []
            elif g_index > 0:
                vals = [value[0] for value in results]
                return vals[0][g_index]
        else:
            QUERY_LOG.error(
                "Impossible de demander un indice autre qu'un "
                "entier ou exceptionnellement 'all'."
            )
            raise ValueError(
                f"Impossible de récupérer l'indice {g_index} "
                f"du résultat de la requête select."
            )
        return None

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

__all__ = ["MySQLConnexion"]

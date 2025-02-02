from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Union, Sequence

from OncheDatabase.tools.MySQLConnexion import QUERY_LOG, MySQLConnexion
from OncheDatabase.elements.Table import Table


@dataclass(init=True, repr=True, order=False, slots=True)
class Database(MySQLConnexion):
    name: str = field(
        default="Onche", init=True, repr=True, hash=True,
        metadata={"description": "nom de la base de donnée"}
    )
    size: float = field(
        init=False, repr=True, hash=True,
        metadata={"description": "taille de la base de donnée en Mo"}
    )

    tables: Sequence[Table] = field(
        init=False, repr=True, hash=True,
        metadata={"description": "liste des tables dans une base de donné"}
    )

    def __post_init__(self) -> None:
        MySQLConnexion.__post_init__(self)
        self.database = self.name
        self.get_size()
        self.get_all_tables()

    def get_size(self) -> None:
        self.size = float(self.query("SELECT SUM(data_length + index_length) / 1024 / 1024 AS size_in_mb"
                               " FROM information_schema.tables"
                               " WHERE table_schema = %s;",
                               (self.name,)
                               )[0])

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
                           f"sur {nm}.")
            return self.query(query)

    def get_all_tables(self) -> Sequence[Table]:
        """
        Renvoie toutes les tables
        :return:
        """
        query = ("SELECT table_name FROM INFORMATION_SCHEMA.TABLES "
                 "WHERE table_schema = %s")
        tables = self.query(query, (self.name,))
        table_list = []
        for table in tables:
            table_list.append(Table(name=table, parent_database=self.name))
        return table_list

    @property
    def database_size(self) -> float:
        return self.size

    @property
    def database_name(self) -> str:
        return self.name

    def __str__(self) -> str:
        return (f"{self.name} size : {self.database_size}Mo\n"
                f"{[val for val in self.tables]}")

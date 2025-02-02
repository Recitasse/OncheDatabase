from dataclasses import dataclass, field, InitVar
from typing import Dict, Any, Optional

from OncheDatabase.tools.MySQLConnexion import QUERY_LOG, MySQLConnexion


@dataclass(init=True, repr=True, order=False, slots=True)
class Table(MySQLConnexion):
    parent_database: InitVar[str] = field(
        default=None, init=True, repr=True, hash=True,
        metadata={"description": "nom de la table"}
    )

    name: Optional[str] = field(
        default=None, init=True, repr=True, hash=True,
        metadata={"description": "nom de la table"}
    )

    infos: Dict[str, Any] = field(
        init=False, repr=True, hash=True,
        metadata={"description": "info d'une ligne dans la table"}
    )

    def __post_init__(self, parent_database: str) -> None:
        super().__init__()
        self.database = parent_database
        from OncheDatabase.tools.DatabaseManager import DatabaseManager
        if parent_database not in DatabaseManager():
            QUERY_LOG.warning(f'la base de donnée {parent_database} ne semble pas exister')
        self.get_infos()

    def get_infos(self) -> None:
        query = f"DESCRIBE {self.name}"
        columns = self.query(query)
        print(f"Colonnes de la table {self.name}:")
        for column in columns:
            print(f"Column: {column[0]}")

        query = ("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY "
                 "FROM INFORMATION_SCHEMA.COLUMNS) "
                 "WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s")
        column_details = self.query(query, (self.name, self.database))

        for column in column_details:
            print(f"Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}, Default: {column[3]}, Key: {column[4]}")
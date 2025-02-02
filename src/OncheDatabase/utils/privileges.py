from enum import StrEnum


class Privileges(StrEnum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    ALL = "ALL"
    GRANT = "WITH GRANT OPTION"

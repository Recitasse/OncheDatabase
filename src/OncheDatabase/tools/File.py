from pathlib import Path
from abc import ABC, abstractmethod


class File(ABC, str):
    def __new__(cls, file_path: str) -> str:
        """
        Init file_path
        :param file_path: Le path du fichier
        """
        if not (Path(file_path).is_file() or Path(file_path).exists()):
            raise FileNotFoundError(f"Le fichier donné est invalide")
        cls._check_file_extension(file_path)
        return file_path


    @classmethod
    @abstractmethod
    def _check_file_extension(cls, path: str) -> None:
        """
        Check le fichier
        :param path:
        :return:
        """
        ...


class SqlFile(File):
    @classmethod
    def _check_file_extension(cls, path: str) -> None:
        if Path(path).suffix != ".sql":
            raise TypeError(f"L'extension du fichier est incohérent")

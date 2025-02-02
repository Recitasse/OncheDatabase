"""
Les exceptions pour les droits
"""

class PrivilegeNotFound(Exception):
    """
    Exception pour les erreurs de privilège d'un utilisateur MySQL
    """
    def __init__(self, privilege, message="Le privilège n'existe pas"):
        self.privilege = privilege
        self.message = f"Erreur : {message} -> {privilege}"
        super().__init__(self.message)

class PrivilegeErreur(Exception):
    """
    Exception pour les erreurs de privilège d'un utilisateur MySQL
    """
    def __init__(self,
                 privilege, message="L'utilisateur n'a pas le privilège'"):
        self.privilege = privilege
        self.message = f"Erreur : {message} -> {privilege}"
        super().__init__(self.message)
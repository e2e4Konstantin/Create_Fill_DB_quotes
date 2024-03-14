from typing import NamedTuple


class AccessData(NamedTuple):
    """ Доступ соединения с БД."""
    host: str = None
    port: int = 5432
    dbname: str = None
    user: str = None
    password: str = None

    def __repr__(self) -> str:
        return f"<AccessData: {self.host}, port={self.port}, dbname={self.dbname}, user={self.user}, password={self.password}>"

    def __str__(self) -> str:
        return f"<AccessData: {self.host!r}, port={self.port}, dbname={self.dbname!r}>"


# Данные для коннектов
db_access: dict[str: AccessData] = {
    "vlad": AccessData(
        host='172.16.49.193', dbname='postgres', user='read_write', password='read_write'
        ),

    "normative": AccessData(
        host='192.168.23.3', dbname='normativ', user='read_larix', password='read_larix'
        )
}

if __name__ == "__main__":
    
    print(*db_access['vlad'])
    print(db_access['vlad'])
    
    
    

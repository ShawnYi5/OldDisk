import sqlalchemy
from sqlalchemy import orm

db_connect_str = 'postgresql+psycopg2://postgres:f@127.0.0.1:21115/disksnapshotservice'
create_engine = sqlalchemy.create_engine(db_connect_str, echo=False, pool_size=20, )
session_maker = orm.sessionmaker(bind=create_engine)


class SessionForRead(object):
    """读session"""

    def __init__(self, session=None, close_when_exit=True):
        if session is None:
            self.session = session_maker()
        else:
            self.session = session
        self.close_when_exit = close_when_exit

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.close_when_exit:
            self.session.close()


class SessionForReadWrite(object):
    """读写session"""

    def __init__(self, session=None, close_when_exit=True):
        if session is None:
            self.session = session_maker()
        else:
            self.session = session
        self.close_when_exit = close_when_exit

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            if self.close_when_exit:
                self.session.close()


class SessionWithTrans(object):
    """带事务的session"""

    def __init__(self):
        self.session = session_maker()
        self.session_trans = None

    def __enter__(self):
        self.session_trans = self.session.begin()
        return self.session_trans.__enter__().session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.session_trans is not None:
                self.session_trans.__exit__(exc_type, exc_val, exc_tb)
                self.session_trans = None
        finally:
            self.session.close()

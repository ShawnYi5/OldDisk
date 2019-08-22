from sqlalchemy import Column, String, DECIMAL, ForeignKey, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


def obj_to_dict(self):
    """obj —> dict"""

    return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


# obj —> dict 应用到 Base
Base.obj_to_dict = obj_to_dict


class Journal(Base):
    __tablename__ = 'journal'

    # operation_type:
    TYPE_NORMAL_CREATE = 'nc'
    TYPE_DESTROY = 'd'
    TYPE_CREATE_FROM_QCOW = 'cfq'
    TYPE_CREATE_FROM_CDP = 'cfc'

    JOURNAL_CREATE_TYPES = (
        TYPE_CREATE_FROM_CDP,
        TYPE_CREATE_FROM_QCOW,
        TYPE_NORMAL_CREATE,
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    produced_timestamp = Column(DECIMAL(16, 6), nullable=False)
    consumed_timestamp = Column(DECIMAL(16, 6), nullable=True)
    token = Column(String(32), unique=True, nullable=False)
    tree_ident = Column(String(32), nullable=False)
    operation_str = Column(String, nullable=False)
    operation_type = Column(String(3), nullable=False)  # operation_type 为枚举类型
    children_idents = Column(String(255), nullable=True)


class SnapshotStorage(Base):
    __tablename__ = 'snapshot_storage'

    # type:
    TYPE_QCOW = 'q'
    TYPE_CDP = 'c'

    # status:
    STATUS_CREATING = 'c'
    STATUS_WRITING = 'r'
    STATUS_HASHING = 'h'
    STATUS_STORAGE = 's'
    STATUS_RECYCLING = 'r'
    STATUS_DELETED = 'd'

    INVALID_STORAGE_STATUS = (
        STATUS_RECYCLING,
        STATUS_DELETED,
    )

    CREATING_AND_WRITING = (
        STATUS_CREATING,
        STATUS_WRITING,
    )

    ident = Column(String(32), primary_key=True, unique=True, nullable=False)
    parent_ident = Column(String(32), ForeignKey("snapshot_storage.ident"), nullable=True)
    parent_timestamp = Column(DECIMAL(16, 6), nullable=True)

    type = Column(String(1), nullable=False)  # type 为枚举类型
    disk_bytes = Column(BigInteger, nullable=False)
    status = Column(String(1), nullable=False)  # status 为枚举类型
    image_path = Column(String(250), nullable=False)
    new_storage_size = Column(BigInteger, nullable=True)
    start_timestamp = Column(DECIMAL(16, 6), nullable=True)
    finish_timestamp = Column(DECIMAL(16, 6), nullable=True)
    tree_ident = Column(String(40), nullable=False)
    file_level_deduplication = Column(Boolean, nullable=True)
    hash = relationship("Hash")


class Hash(Base):
    __tablename__ = 'hash'

    # version:
    VERSION_MD4_CRC32 = '1'

    # method:
    METHOD_INCREMENT = 'i'
    METHOD_FULL = 'f'

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    storage_ident = Column(String(32), ForeignKey("snapshot_storage.ident"), nullable=False)
    timestamp = Column(DECIMAL(16, 6), nullable=False)
    # Hash算法
    version = Column(String(1), nullable=False)  # version 为枚举类型
    # 全量、增量
    method = Column(String(1), nullable=False)  # method 为枚举类型
    path = Column(String(250), nullable=False)

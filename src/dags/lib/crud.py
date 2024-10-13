from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from pymongo.mongo_client import MongoClient
from lib.utils import CustomSession


class Saver(ABC):

    @abstractmethod
    def insert(self, *args, **kwargs):
        pass


class Reader(ABC):

    @abstractmethod
    def list(self, *args, **kwargs):
        pass


class PgSaver(Saver):

    def insert(self, conn: Connection, query: str, params: Dict) -> None:
        with conn.cursor() as cur:
            cur.execute(query, params)


class PgReader(Reader):

    def list(self, conn: Connection, query: str, model: ModelMetaclass, params: Dict = None) -> List[BaseModel]:
        with conn.cursor(row_factory=class_row(model)) as cur:
            cur.execute(query, params)
            objs = cur.fetchall()
        return objs

    def retrieve(self, conn: Connection, query: str, model: ModelMetaclass, params: Dict = None) -> Optional[BaseModel]:
        with conn.cursor(row_factory=class_row(model)) as cur:
            cur.execute(query, params)
            obj = cur.fetchone()
        return obj


class MongoReader(Reader):

    def list(self, conn: MongoClient, collection: str, model: ModelMetaclass, filter: Dict = None, sort: List = None, limit: int = None) -> List[BaseModel]:
        docs = [
            model(**doc) for doc in conn.get_database().get_collection(collection).find(filter=filter, sort=sort, limit=limit)
        ]
        return docs


class HttpReader(Reader):

    def list(self, conn: CustomSession, method: str, model: ModelMetaclass, params: Dict = None) -> List[BaseModel]:
        resp = conn.get(method, params=params)
        if not resp.content:
            return []
        return [model(**item) for item in resp.json()]

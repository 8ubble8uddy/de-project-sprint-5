from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, Generator, Union
from urllib.parse import quote_plus as quote

import psycopg
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from pymongo.mongo_client import MongoClient
from lib.utils import CustomSession


class Connect(ABC):
    
    @abstractmethod
    def connection(self, *args, **kwargs) -> Generator:
        ...


class MongoConnect(Connect):
    def __init__(self,
                 cert_path: str,
                 user: str,
                 pw: str,
                 host: str,
                 rs: str,
                 auth_db: str,
                 main_db: str
                 ) -> None:
 
        self.user = user
        self.pw = pw
        self.host = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/{db}?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.host,
            rs=self.replica_set,
            db=self.main_db,
            auth_src=self.auth_db)

    def client(self) -> MongoClient:
        return MongoClient(self.url(), tlsCAFile=self.cert_path)

    @contextmanager
    def connection(self) -> Generator[MongoClient, None, None]:
        conn = self.client()
        try:
            yield conn
        except Exception as e:
            raise e
        finally:
            conn.close()


class PgConnect(Connect):
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    def client(self) -> psycopg.Connection:
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = self.client()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class HttpConnect(Connect):
    def __init__(self, host: str, port: str, protocol: str, headers: Dict) -> None:
        self.host = host
        self.port = int(port)
        self.protocol = protocol
        self.headers = headers

    def url(self) -> str:
        return '{protocol}://{host}:{port}'.format(
            protocol=self.protocol,
            host=self.host,
            port=self.port)

    def client(self) -> CustomSession:
        return CustomSession(self.url(), self.headers)

    @contextmanager
    def connection(self) -> Generator[CustomSession, None, None]:
        conn = self.client()
        try:
            yield conn
        except Exception as e:
            raise e
        finally:
            conn.close()


class ConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg

    @staticmethod
    def mongo_conn(var_key: str) -> MongoConnect:
        params = Variable.get(var_key, deserialize_json=True)

        mongo = MongoConnect(params['certificate_path'],
                             params['user'],
                             params['password'],
                             params['host'],
                             params['replica_set'],
                             params['database_name'],
                             params['database_name'])

        return mongo

    @staticmethod
    def http_conn(conn_id: str) -> MongoConnect:
        conn = BaseHook.get_connection(conn_id)

        if '://' in conn.host:
            conn.schema, conn.host = conn.host.split('://')

        if conn.schema == 'https':
            conn.port = 443
        
        http = HttpConnect(conn.host,
                           conn.port or '80',
                           conn.schema or 'http',
                           conn.extra_dejson)

        return http

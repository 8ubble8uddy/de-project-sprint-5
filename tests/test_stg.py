import unittest
import requests


def submit(t_code):
    r = requests.post(
        'http://localhost:3002',
        json={
            "code": "SELECT",
            "test": t_code
            },
        timeout=5)


    stderr, stdout = r.json()['stderr'], r.json()['stdout']

    print(stderr)
    print(stdout)

    return stdout


class TestStgSchemaInit(unittest.TestCase):
    """Проектирование слоя STG."""

    def test_ddl_bonussystem(self):
        self.assertIn('S3Hlgxf3Vd', submit('de05030701'))

    def test_ddl_ordersystem(self):
        self.assertIn('OIoYDT7RQC', submit('de05030702'))
    
    def test_constraints_ordersystem_object_id_uindex(self):
        self.assertIn('Ve7J48uY2K', submit('de05030703'))


class TestBonusSystemOriginToStg(unittest.TestCase):
    """Перенос данных из системы бонусов PostgreSQL в STG."""

    def test_pg_connection_bonussystem(self):
        self.assertIn('gESZ89Tpop', submit('de05040501'))
    
    def test_load_bonussystem_ranks(self):
        self.assertIn('WHBkgRkvLo', submit('de05040502'))

    def test_load_bonussystem_users(self):
        self.assertIn('lgkXY8KtCn', submit('de05040503'))

    def test_load_bonussystem_events(self):
        self.assertIn('mgXgcqQzFv', submit('de05040505'))


class TestOrderSystemOriginToStg(unittest.TestCase):
    """Перенос данных из системы заказов MongoDB в STG."""

    def test_mongo_connection_ordersystem(self):
        self.assertIn('WXcbW1NLh9', submit('de05040601'))

    def test_ddl_ordersystem(self):
        self.assertIn('xLsFC21xuE', submit('de05040602'))

    def test_load_ordersystem_restaurants(self):
        self.assertIn('k2Hetyy0nu', submit('de05040603'))

    def test_load_ordersystem_users(self):
        self.assertIn('ChGN03te37', submit('de05040604'))

    def test_load_ordersystem_orders(self):
        self.assertIn('ngY7uVwwuM', submit('de05040605'))

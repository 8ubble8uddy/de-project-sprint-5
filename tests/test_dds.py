import unittest
import requests


def submit(t_code):
    r = requests.post(
        'http://localhost:3002',
        json={
            "code": "SELECT",
            "test": t_code
            },
        timeout=10)

    stderr, stdout = r.json()['stderr'], r.json()['stdout']

    print(stderr)
    print(stdout)

    return stdout


class TestDdsSchemaInit(unittest.TestCase):
    """Проектирование слоя DDS."""

    def test_ddl_dds_schema(self):
        self.assertIn('7lcOGpj4ZL', submit('de05030801'))

    def test_ddl_dm_users(self):
        self.assertIn('g0MSs4ez0v', submit('de05030802'))
    
    def test_ddl_dm_restaurants(self):
        self.assertIn('h5xVUDD1Wm', submit('de05030803'))

    def test_ddl_dm_products(self):
        self.assertIn('yCYrkJMm2Z', submit('de05030804'))

    def test_constraints_dm_products_restaurant_id_fkey(self):
        self.assertIn('rgNx3z8aKm', submit('de05030805'))

    def test_ddl_dm_timestamps(self):
        self.assertIn('tIVtQGEgoL', submit('de05030806'))

    def test_ddl_dm_orders(self):
        self.assertIn('wgfELwIQ0E', submit('de05030807'))

    def test_constraints_dm_orders_fkeys(self):
        self.assertIn('FyKdrGMogs', submit('de05030808'))

    def test_ddl_fct_product_sales(self):
        self.assertIn('udZMKdxadM', submit('de05030809'))

    def test_constraints_fct_product_sales_fkeys(self):
        self.assertIn('NVyO1i2Dr5', submit('de05030810'))


class TestStgOriginToDds(unittest.TestCase):
    """Перенос данных из STG в DDS."""

    def test_ddl_srv_wf_settings(self):
        self.assertIn('RqgcX9kTN6', submit('de05040701'))
    
    def test_load_dm_users(self):
        self.assertIn('kX3riKpibW', submit('de05040702'))

    def test_load_dm_restaurants(self):
        self.assertIn('mgXgcqQzFv', submit('de05040703'))

    def test_load_dm_timestamps(self):
        self.assertIn('WXcbW1NLh9', submit('de05040704'))

    def test_load_dm_products(self):
        self.assertIn('y7M8bxX1z9', submit('de05040705'))

    def test_load_dm_orders(self):
        self.assertIn('8i8NjzMWsa', submit('de05040706'))

    def test_load_fct_product_sales(self):
        self.assertIn('jemju9gmX7', submit('de05040707'))

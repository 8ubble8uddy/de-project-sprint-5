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


class TestCdmSchemaInit(unittest.TestCase):
    """Проектирование слоя CDM."""

    def test_ddl_dm_settlement_report(self):
        self.assertIn('xjnEv7JXWN', submit('de05030601'))

    def test_constaints_dm_settlement_report_pkey(self):
        self.assertIn('5zF17A2Gzb', submit('de05030602'))
    
    def test_constaints_dm_settlement_report_settlement_date_check(self):
        self.assertIn('ZBfLxNLryk', submit('de05030603'))

    def test_constaints_dm_settlement_metrics_check(self):
        self.assertIn('qUpu6QoZs9', submit('de05030604'))

    def test_constaints_dm_settlement_report_settlement_date_restaurant_id_unique(self):
        self.assertIn('YLalElZtMP', submit('de05030605'))


class TestDdsOriginToCdm(unittest.TestCase):
    """Перенос данных из DDS в CDM."""

    def test_load_dm_settlement_report(self):
        self.assertIn('QAJuAX0xGs', submit('de05040801'))

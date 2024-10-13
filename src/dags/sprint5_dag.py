import logging
from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from dwh import stg_1, stg_2, stg_3, dds, cdm
from lib.schema_init import SchemaDdl
from lib.connect import ConnectionBuilder, PgConnect, MongoConnect, HttpConnect


# Создаём подключения к источникам и DWH.
dwh_pg_connect = ConnectionBuilder.pg_conn(conn_id='PG_WAREHOUSE_CONNECTION')
origin_pg_connect = ConnectionBuilder.pg_conn(conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
origin_mongo_connect = ConnectionBuilder.mongo_conn(var_key='MONGO_ORIGIN_ORDER_SYSTEM_PARAMS')
origin_http_connect = ConnectionBuilder.http_conn(conn_id='HTTP_ORIGIN_DELIVERY_SYSTEM_CONNECTION')

# Объявляем словарь с аргументами, которые передаются при запуске задач.
args = {
    'dwh_pg_connect': dwh_pg_connect,
    'origin_pg_connect': origin_pg_connect,
    'origin_mongo_connect': origin_mongo_connect,
    'origin_http_connect': origin_http_connect,
    'dags_dir': '/lessons/dags',
    'log': logging.getLogger(__name__),
    'trigger_rule': TriggerRule.ALL_DONE
}

# Объявляем DAG с настройками задач по умолчанию.
@dag(
    'settlements_mart',
    default_args=args,
    schedule_interval='0/15 * * * *',
    start_date=days_ago(31),
    catchup=False,
    tags=['sprint5', 'stg', 'dds', 'cdm'],
    is_paused_upon_creation=True,
    max_active_runs=1
)
def settlements_mart_dag():

    # Создаем структуру таблиц DWH.
    @task_group(group_id='INIT')
    def init_dwh():

        @task(task_id='stg')
        def init_stg(dwh_pg_connect: PgConnect, dags_dir: str, log: logging.Logger, prev_start_date_success: datetime = None):
            stg_schema = SchemaDdl(dwh_pg_connect, log)
            stg_schema.init(dags_dir + '/dwh/stg/ddl', prev_start_date_success)

        @task(task_id='dds')
        def init_dds(dwh_pg_connect: PgConnect, dags_dir: str, log: logging.Logger, prev_start_date_success: datetime = None):
            dds_schema = SchemaDdl(dwh_pg_connect, log)
            dds_schema.init(dags_dir + '/dwh/dds/ddl', prev_start_date_success)

        @task(task_id='cdm')
        def init_cdm(dwh_pg_connect: PgConnect, dags_dir: str, log: logging.Logger, prev_start_date_success: datetime = None):
            cdm_schema = SchemaDdl(dwh_pg_connect, log)
            cdm_schema.init(dags_dir + '/dwh/cdm/ddl', prev_start_date_success)

        stg_init = init_stg()
        dds_init = init_dds()
        cdm_init = init_cdm()

        [stg_init, dds_init, cdm_init]


    # Заполняем STG-слой данными из подсистемы бонусных расчётов компании.
    @task_group(group_id='STG-1')
    def load_stg_1():

        @task(task_id='bonussystem_ranks')
        def load_bonus_system_ranks(origin_pg_connect: PgConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            ranks_to_stg = stg_1.RanksLoader(origin_pg_connect, dwh_pg_connect, log)
            ranks_to_stg.run()
        
        @task(task_id='bonussystem_users')
        def load_bonus_system_users(origin_pg_connect: PgConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            users_to_stg = stg_1.UsersLoader(origin_pg_connect, dwh_pg_connect, log)
            users_to_stg.run()

        @task(task_id='bonussystem_events')
        def load_bonus_system_events(origin_pg_connect: PgConnect, dwh_pg_connect: PgConnect, log: logging.Logger, dag: DAG = None):
            events_to_stg = stg_1.EventsLoader(origin_pg_connect, dwh_pg_connect, log)
            events_to_stg.run(dag.start_date)

        rank_loader = load_bonus_system_ranks()
        user_loader = load_bonus_system_users()
        event_loader = load_bonus_system_events()

        [rank_loader, user_loader, event_loader]


    # Заполняем STG-слой данными из подсистемы обработки заказов.
    @task_group(group_id='STG-2')
    def load_stg_2():

        @task(task_id='ordersystem_restaurants')
        def load_order_system_restaurants(origin_mongo_connect: MongoConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            restaurants_to_stg = stg_2.RestaurantsLoader(origin_mongo_connect, dwh_pg_connect, log)
            restaurants_to_stg.run()

        @task(task_id='ordersystem_orders')
        def load_order_system_orders(origin_mongo_connect: MongoConnect, dwh_pg_connect: PgConnect, log: logging.Logger, dag: DAG = None):
            orders_to_stg = stg_2.OrdersLoader(origin_mongo_connect, dwh_pg_connect, log)
            orders_to_stg.run(dag.start_date)

        @task(task_id='ordersystem_users')
        def load_order_system_users(origin_mongo_connect: MongoConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            users_to_stg = stg_2.UsersLoader(origin_mongo_connect, dwh_pg_connect, log)
            users_to_stg.run()

        restaurant_loader = load_order_system_restaurants()
        order_loader = load_order_system_orders()
        user_loader = load_order_system_users()

        [restaurant_loader, order_loader, user_loader]
    

    # Заполняем STG-слой данными из подсистемы доставки заказов.
    @task_group(group_id='STG-3')
    def load_stg_3():

        @task(task_id='deliverysystem_restaurants')
        def load_delivery_system_restaurants(origin_http_connect: HttpConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            restaurants_to_stg = stg_3.RestaurantsLoader(origin_http_connect, dwh_pg_connect, log)
            restaurants_to_stg.run()

        @task(task_id='deliverysystem_couriers')
        def load_delivery_system_couriers(origin_http_connect: HttpConnect, dwh_pg_connect: PgConnect, log: logging.Logger):
            couriers_to_stg = stg_3.CouriersLoader(origin_http_connect, dwh_pg_connect, log)
            couriers_to_stg.run()

        @task(task_id='deliverysystem_deliveries')
        def load_delivery_system_deliveries(origin_http_connect: HttpConnect, dwh_pg_connect: PgConnect, log: logging.Logger, dag: DAG = None):
            deliveries_to_stg = stg_3.DeliveriesLoader(origin_http_connect, dwh_pg_connect, log)
            deliveries_to_stg.run(dag.start_date)

        restaurant_loader = load_delivery_system_restaurants()
        courier_loader = load_delivery_system_couriers()
        delivery_loader = load_delivery_system_deliveries()

        [restaurant_loader, courier_loader, delivery_loader]


    # Переносим данные из STG-слоя в DDS-слой.
    @task_group(group_id='DDS')
    def load_dds():

        @task(task_id='dm_users')
        def load_dimension_users(dwh_pg_connect: PgConnect, log: logging.Logger):
            users_to_dds = dds.UsersLoader(dwh_pg_connect, dwh_pg_connect, log)
            users_to_dds.run()

        @task(task_id='dm_restaurants')
        def load_dimension_restaurants(dwh_pg_connect: PgConnect, log: logging.Logger):
            restaurants_to_dds = dds.RestaurantsLoader(dwh_pg_connect, dwh_pg_connect, log)
            restaurants_to_dds.run()

        @task(task_id='dm_timestamps')
        def load_dimension_timestamps(dwh_pg_connect: PgConnect, log: logging.Logger):
            timestamps_to_dds = dds.TimestampsLoader(dwh_pg_connect, dwh_pg_connect, log)
            timestamps_to_dds.run()

        @task(task_id='dm_couriers')
        def load_dimension_couriers(dwh_pg_connect: PgConnect, log: logging.Logger):
            couriers_to_dds = dds.CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
            couriers_to_dds.run()

        @task(task_id='dm_products')
        def load_dimension_products(dwh_pg_connect: PgConnect, log: logging.Logger):
            products_to_dds = dds.ProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
            products_to_dds.run()

        @task(task_id='dm_orders')
        def load_dimension_orders(dwh_pg_connect: PgConnect, log: logging.Logger):
            orders_to_dds = dds.OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
            orders_to_dds.run()

        @task(task_id='fct_product_sales')
        def load_facts_product_sales(dwh_pg_connect: PgConnect, log: logging.Logger):
            product_sales_to_dds = dds.ProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
            product_sales_to_dds.run()

        @task(task_id='fct_deliveries')
        def load_facts_deliveries(dwh_pg_connect: PgConnect, log: logging.Logger):
            deliveries_to_dds = dds.DeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
            deliveries_to_dds.run()

        user_loader = load_dimension_users()
        restaurant_loader = load_dimension_restaurants()
        timestamp_loader = load_dimension_timestamps()
        courier_loader = load_dimension_couriers()
        product_loader = load_dimension_products()
        order_loader = load_dimension_orders()
        product_sale_loader = load_facts_product_sales()
        delivery_loader = load_facts_deliveries()

        restaurant_loader >> product_loader
        [user_loader, restaurant_loader, timestamp_loader] >> order_loader
        [product_loader, order_loader] >> product_sale_loader
        [order_loader, courier_loader] >> delivery_loader


    # Заполняем витрины в слое CDM.
    @task_group(group_id='CDM')
    def load_cdm():

        @task(task_id='dm_settlement_report')
        def load_datamart_settlement_report(dwh_pg_connect: PgConnect, log: logging.Logger):
            settlement_report_to_cdm = cdm.SettlementReportLoader(dwh_pg_connect, dwh_pg_connect, log)
            settlement_report_to_cdm.run()

        @task(task_id='dm_courier_ledger')
        def load_datamart_courier_ledger(dwh_pg_connect: PgConnect, log: logging.Logger):
            courier_ledger_to_cdm = cdm.CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
            courier_ledger_to_cdm.run()

        settlement_report_loader = load_datamart_settlement_report()
        courier_ledger_loader = load_datamart_courier_ledger()

        [settlement_report_loader, courier_ledger_loader]


    # Инициализируем объявленные группы задач.
    dwh_init = init_dwh()
    stg_1_loader = load_stg_1()
    stg_2_loader = load_stg_2()
    stg_3_loader = load_stg_3()
    dds_loader = load_dds()
    cdm_loader = load_cdm()

    # Задаем последовательность выполнения групп задач.
    dwh_init >> [stg_1_loader, stg_2_loader, stg_3_loader] >> dds_loader >> cdm_loader


# Вызываем функцию, описывающую даг.
settlements_mart = settlements_mart_dag()

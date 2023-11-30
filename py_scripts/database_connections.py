#!/usr/bin/python3
import psycopg2

# Создание подключения к PostgreSQL
def create_connection_src():
    conn_src = psycopg2.connect(database = "bank",
                        host =     "host.ru",
                        user =     "bank_etl",
                        password = "bank_etl_password",
                        port =     "5432")
    return conn_src
    
def create_connection_dwh():    
    conn_dwh = psycopg2.connect(database = "dwh",
                        host =     "dwh.ru",
                        user =     "user",
                        password = "user_password",
                        port =     "5432")
    return conn_dwh
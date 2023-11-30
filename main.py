#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import subprocess


print('- Создание подключений')
## ## Создание подключения к PostgreSQL
from py_scripts.database_connections import create_connection_src, create_connection_dwh

conn_src = create_connection_src()
conn_dwh = create_connection_dwh()

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

## ## Первичное заполнение meta_maxdate
print('- Первичное заполнение meta_maxdate')
if not os.path.exists("/home/project/py_scripts/flag_for_meta_maxdate.txt"):
    cursor_dwh.execute("""INSERT INTO deaise.daar_meta_maxdate (schema_name, table_name, max_update_dt)
                            VALUES ('deaise', 'daar_stg_terminals', to_timestamp('1000-01-01', 'YYYY-MM-DD'))""")
    cursor_dwh.execute("""INSERT INTO deaise.daar_meta_maxdate (schema_name, table_name, max_create_dt, max_update_dt)                      
                           VALUES ('deaise', 'daar_stg_cards', to_timestamp('1000-01-01', 'YYYY-MM-DD'), to_timestamp('1000-01-01', 'YYYY-MM-DD'))""")
    cursor_dwh.execute("""INSERT INTO deaise.daar_meta_maxdate (schema_name, table_name, max_create_dt, max_update_dt)            
                            VALUES ('deaise', 'daar_stg_accounts', to_timestamp('1000-01-01', 'YYYY-MM-DD'), to_timestamp('1000-01-01', 'YYYY-MM-DD'))""")
    cursor_dwh.execute("""INSERT INTO deaise.daar_meta_maxdate (schema_name, table_name, max_create_dt, max_update_dt)           
                            VALUES ('deaise', 'daar_stg_clients', to_timestamp('1000-01-01', 'YYYY-MM-DD'), to_timestamp('1000-01-01', 'YYYY-MM-DD'))""")

#####    #####    -----------------------------------   #####    #####   

## << Очистка Stage >>
print('1. Очистка stage')

cursor_dwh.execute( "DELETE FROM deaise.daar_stg_terminals")
cursor_dwh.execute( "DELETE FROM deaise.daar_stg_cards")
cursor_dwh.execute( "DELETE FROM deaise.daar_stg_accounts")
cursor_dwh.execute( "DELETE FROM deaise.daar_stg_clients")
cursor_dwh.execute( "DELETE FROM deaise.daar_stg_passport_blacklist")
cursor_dwh.execute( "DELETE FROM deaise.daar_stg_transactions")
cursor_dwh.execute( "DELETE FROM deaise.daar_meta_deleted")

## ## << Загрузка данных в Stage >>
print('2. Загрузка в stage')

## -- stg_terminals --                     
print('2.1 stg_terminals')
found_file = False
for file in os.listdir("/home/project/"):
    if 'terminals' in file and not os.path.exists("/home/project/archive/" + file + ".backup"):
        found_file = True
        df = pd.read_excel("/home/project/" + file)
        cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_terminals(     
                                    terminal_id,
                                    terminal_type,
                                    terminal_city,
                                    terminal_address) 
                                   VALUES( %s, %s, %s, %s)""", df.values.tolist() )
if not found_file:
    print('--** Файл terminals_DDMMYYYY.xlsx отсутствует **--')

## -- stg_cards -- 
print('2.2 stg_cards')
cursor_dwh.execute( """ SELECT 
                            COALESCE( max_create_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') ),
                            COALESCE( max_update_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') )
                        FROM deaise.daar_meta_maxdate
                        WHERE schema_name = 'deaise' AND table_name = 'daar_stg_cards' """)               
result = cursor_dwh.fetchone()                    
cursor_src.execute( """ SELECT 
                            card_num,
                            account,
                            create_dt,
                            update_dt
                        FROM bank.info.cards
                        WHERE create_dt > %s
                           OR update_dt > %s""", result)
                    
records = cursor_src.fetchall()
df = pd.DataFrame( records )
cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_cards( 
                            card_num,
                            account_num,
                            create_dt,
                            update_dt ) 
                           VALUES( %s, %s, %s, %s )""", df.values.tolist() )      

                          
## -- stg_accounts --                          
print('2.3 stg_accounts')
cursor_dwh.execute( """ SELECT 
                            COALESCE( max_create_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') ),
                            COALESCE( max_update_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') )
                        FROM deaise.daar_meta_maxdate
                        WHERE schema_name = 'deaise' AND table_name = 'daar_stg_accounts' """)
result = cursor_dwh.fetchone()                    
cursor_src.execute( """ SELECT 
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt
                        FROM bank.info.accounts
                        WHERE create_dt > %s
                           OR update_dt > %s""", result)
                           
records = cursor_src.fetchall()
df = pd.DataFrame( records )
cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_accounts( 
                            account_num,
                            valid_to,
                            client,
                            create_dt,
                            update_dt  ) 
                           VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() ) 
                           
## -- stg_clients --                                                
print('2.4 stg_clients')
cursor_dwh.execute( """ SELECT 
                            COALESCE( max_create_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') ),
                            COALESCE( max_update_dt, TO_DATE('1000-01-01', 'YYYY-MM-DD') )
                        FROM deaise.daar_meta_maxdate
                        WHERE schema_name = 'deaise' AND table_name = 'daar_stg_clients' """)
result = cursor_dwh.fetchone() 
cursor_src.execute( """ SELECT 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt
                        FROM bank.info.clients
                        WHERE create_dt > %s
                           OR update_dt > %s""", result)
                           
records = cursor_src.fetchall()
df = pd.DataFrame( records )
cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_clients( 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt ) 
                           VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )  


## -- stg_passport_blacklist --
print('2.5 stg_passport_blacklist')
found_file = False
for file in os.listdir("/home/project/"):
    if 'passport_blacklist' in file:
        found_file = True
        df = pd.read_excel("/home/project/" + file)
        df = df [['passport','date']]   
        cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_passport_blacklist(
                                    passport_num,
                                    entry_dt)
                                   VALUES(%s, %s )""", df.values.tolist() ) 
if not found_file:
    print('--** Файл passport_blacklist_DDMMYYYY.xlsx отсутствует **--')    


## -- stg_transactions --
print('2.6 stg_transactions')
found_file = False 
for file in os.listdir("/home/deaise/daar/project/"):
    if 'transactions' in file:
        found_file = True
        df = pd.read_csv("/home/project/" + file, sep=';')
        df['amount'] = df['amount'].str.replace(',', '.')
        df = df[['transaction_id','transaction_date','card_num','oper_type','amount','oper_result','terminal']]         
        cursor_dwh.executemany( """INSERT INTO deaise.daar_stg_transactions(
                                        trans_id,
                                        trans_date,
                                        card_num,
                                        oper_type,
                                        amt,
                                        oper_result,
                                        terminal)
                                   VALUES( %s, %s, %s, %s, %s , %s, %s )""", df.values.tolist() )
if not found_file:
    print('--** Файл transactions_DDMMYYYY.xlsx отсутствует **--')    

## ## << Захват в стейджинг ключей из источника полным срезом для вычисления удалений >>
print('3. Захват ключей для вычисления удалений')

# -- terminals -- 
print('3.1 terminals')
for file in os.listdir("/home/project/"):
    if 'terminals' in file:
        df = pd.read_excel("/home/project/" + file)
        df = df[['terminal_id']]
        cursor_dwh.executemany( """INSERT INTO daar_meta_deleted( terminal_id ) 
                                   VALUES( %s )""", df.values.tolist() )
                           
## -- cards --                            
print('3.2 cards')
cursor_src.execute( """ SELECT card_num FROM bank.info.cards """)
data_to_insert = [row[0] for row in cursor_src.fetchall()]
cursor_dwh.executemany( """ INSERT INTO deaise.daar_meta_deleted ( card_num )
                            VALUES (%s)""", [(card_num,) for card_num in data_to_insert])

## -- accounts --                              
print('3.3 accounts')
cursor_src.execute( """ SELECT account FROM bank.info.accounts """)
data_to_insert = [row[0] for row in cursor_src.fetchall()]
cursor_dwh.executemany( """ INSERT INTO deaise.daar_meta_deleted ( account_num )
                            VALUES (%s)""", [(account_num,) for account_num in data_to_insert])

## -- clients --                            
print('3.4 clients')
cursor_src.execute( """ SELECT client_id FROM bank.info.clients """)
data_to_insert = [row[0] for row in cursor_src.fetchall()]
cursor_dwh.executemany( """ INSERT INTO deaise.daar_meta_deleted ( client_id )
                            VALUES (%s)""", [(client_id,) for client_id in data_to_insert])


## ## << Загрузка данных в детальный слой DDS (формат SCD2) >>
print('4. Загрузка в DDS')

## -- dwh_dim_terminals --  
print('4.1 dwh_dim_terminals_hist') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            stg.update_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_terminals stg
                        LEFT JOIN deaise.daar_dwh_dim_terminals_hist tgt
                            ON 1=1
                            AND stg.terminal_id = tgt.terminal_id
                        WHERE tgt.terminal_id IS NULL """)

## -- dwh_dim_cards --
print('4.2 dwh_dim_cards_hist') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.card_num,
                            stg.account_num,
                            stg.create_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_cards stg
                        LEFT JOIN deaise.daar_dwh_dim_cards_hist tgt
                            ON 1=1
                            AND stg.card_num = tgt.card_num
                        WHERE tgt.card_num IS NULL """)

## -- dwh_dim_accounts --
print('4.3 dwh_dim_accounts_hist') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            stg.create_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_accounts stg
                        LEFT JOIN deaise.daar_dwh_dim_accounts_hist tgt
                            ON 1=1
                            AND stg.account_num = tgt.account_num
                        WHERE tgt.account_num IS NULL """)

## -- dwh_dim_clients --
print('4.4 dwh_dim_clients_hist') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.create_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_clients stg
                        LEFT JOIN deaise.daar_dwh_dim_clients_hist tgt
                            ON 1=1
                            AND stg.client_id = tgt.client_id
                        WHERE tgt.client_id IS NULL """)

                        
## -- fact_passport_blacklist --  
print('4.5 fact_passport_blacklist') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_fact_passport_blacklist (
                            passport_num,
                            entry_dt)
                        SELECT 
                            stg.passport_num,
                            stg.entry_dt
                        FROM deaise.daar_stg_passport_blacklist stg
                        LEFT JOIN deaise.daar_dwh_fact_passport_blacklist tgt
                            ON 1=1
                            AND stg.passport_num = tgt.passport_num
                        WHERE tgt.passport_num IS NULL """)

## -- fact_transactions --
print('4.6 fact_transactions')
cursor_dwh.execute( """INSERT INTO deaise.daar_dwh_fact_transactions(
                                trans_id,
                                trans_date,
                                card_num,
                                oper_type,
                                amt,
                                oper_result,
                                terminal)
                            SELECT 
                                stg.trans_id,
                                stg.trans_date,
                                stg.card_num,
                                stg.oper_type,
                                stg.amt,
                                stg.oper_result,
                                stg.terminal
                            FROM  deaise.daar_stg_transactions stg   
                            LEFT JOIN deaise.daar_dwh_fact_transactions tgt
                                ON 1=1
                                AND stg.trans_id = stg.trans_id
                                AND stg.trans_date = tgt.trans_date
                            WHERE tgt.trans_id IS NULL """)  


## ## << Обновление данных в детальном слое DDS (формат SCD2)>>  
print('5. Обновление в DDS')       

## -- dwh_dim_terminals --
print('5.1 dwh_dim_terminals_hist ') 
cursor_dwh.execute( """INSERT INTO deaise.daar_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg )
                        SELECT 
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            stg.update_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_terminals stg
                        INNER JOIN deaise.daar_dwh_dim_terminals_hist tgt
                            ON stg.terminal_id = tgt.terminal_id
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                        WHERE ( 1=0
                                OR stg.terminal_type <> tgt.terminal_type OR (stg.terminal_type IS NULL AND tgt.terminal_type IS NOT NULL) OR (stg.terminal_type IS NOT NULL AND tgt.terminal_type IS NULL) 
                                OR stg.terminal_city <> tgt.terminal_city OR (stg.terminal_city IS NULL AND tgt.terminal_city IS NOT NULL) OR (stg.terminal_city IS NOT NULL AND tgt.terminal_city IS NULL)
                                OR stg.terminal_address <> tgt.terminal_address OR (stg.terminal_address IS NULL AND tgt.terminal_address IS NOT NULL) OR (stg.terminal_address IS NOT NULL AND tgt.terminal_address IS NULL))
                                OR tgt.deleted_flg = 'Y' """)
                               
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_terminals_hist tgt
                           SET 
                            effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                            SELECT 
                                tgt.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                stg.update_dt
                            FROM deaise.daar_stg_terminals stg
                            INNER JOIN deaise.daar_dwh_dim_terminals_hist tgt
                                ON stg.terminal_id = tgt.terminal_id
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') 
                            WHERE ( 1=0
                                     OR stg.terminal_type <> tgt.terminal_type OR (stg.terminal_type IS NULL AND tgt.terminal_type IS NOT NULL) OR (stg.terminal_type IS NOT NULL AND tgt.terminal_type IS NULL) 
                                     OR stg.terminal_city <> tgt.terminal_city OR (stg.terminal_city IS NULL AND tgt.terminal_city IS NOT NULL) OR (stg.terminal_city IS NOT NULL AND tgt.terminal_city IS NULL)
                                     OR stg.terminal_address <> tgt.terminal_address OR (stg.terminal_address IS NULL AND tgt.terminal_address IS NOT NULL) OR (stg.terminal_address IS NOT NULL AND tgt.terminal_address IS NULL))
                                    OR tgt.deleted_flg = 'Y' ) tmp
                        WHERE tgt.terminal_id = tmp.terminal_id
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND ( tmp.terminal_type <> tgt.terminal_type OR (tmp.terminal_type IS NULL AND tgt.terminal_type IS NOT NULL) OR (tmp.terminal_type IS NOT NULL AND tgt.terminal_type IS NULL) 
                                OR tmp.terminal_city <> tgt.terminal_city OR (tmp.terminal_city IS NULL AND tgt.terminal_city IS NOT NULL) OR (tmp.terminal_city IS NOT NULL AND tgt.terminal_city IS NULL)
                                OR tmp.terminal_address <> tgt.terminal_address OR (tmp.terminal_address IS NULL AND tgt.terminal_address IS NOT NULL) OR (tmp.terminal_address IS NOT NULL AND tgt.terminal_address IS NULL)
                                OR tgt.deleted_flg = 'Y')""")

## -- dwh_dim_cards --
print('5.2 dwh_dim_cards_hist ') 
cursor_dwh.execute( """INSERT INTO deaise.daar_dwh_dim_cards_hist ( 
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.card_num,
                            stg.account_num,
                            stg.update_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_cards stg
                        INNER JOIN deaise.daar_dwh_dim_cards_hist tgt
                            ON stg.card_num = tgt.card_num
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                        WHERE ( 1=0
                                 OR stg.account_num <> tgt.account_num OR (stg.account_num IS NULL AND tgt.account_num IS NOT NULL) 
                                 OR (stg.account_num IS NOT NULL AND tgt.account_num IS NULL))
                                OR tgt.deleted_flg = 'Y' """)
                                
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_cards_hist tgt
                           SET
                            effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                            SELECT
                                tgt.card_num,
                                stg.account_num,
                                stg.update_dt
                            FROM deaise.daar_stg_cards stg
                            INNER JOIN deaise.daar_dwh_dim_cards_hist tgt
                                ON stg.card_num = tgt.card_num
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            WHERE ( 1=0
                                    OR stg.account_num <> tgt.account_num OR (stg.account_num IS NULL AND tgt.account_num IS NOT NULL) OR (stg.account_num IS NOT NULL AND tgt.account_num IS NULL) )
                                   OR tgt.deleted_flg = 'Y' ) tmp
                        WHERE tgt.card_num = tmp.card_num 
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND ( tmp.account_num <> tgt.account_num OR (tmp.account_num IS NULL AND tgt.account_num IS NOT NULL) 
                                OR (tmp.account_num IS NOT NULL AND tgt.account_num IS NULL)
                                OR tgt.deleted_flg = 'Y')""")

## -- dwh_dim_accounts --
print('5.3 dwh_dim_accounts_hist ') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_accounts_hist ( 
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_accounts stg
                            INNER JOIN deaise.daar_dwh_dim_accounts_hist tgt
                            ON stg.account_num = tgt.account_num
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            WHERE ( 1=0
                                    OR stg.valid_to <> tgt.valid_to OR (stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL) OR (stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL)
                                    OR stg.client <> tgt.client OR (stg.client IS NULL AND tgt.client IS NOT NULL) OR (stg.client IS NOT NULL AND tgt.client IS NULL)) 
                                OR tgt.deleted_flg = 'Y' """)
          
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_accounts_hist tgt
                           SET 
                            effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                            SELECT 
                                tgt.account_num,
                                stg.valid_to,
                                stg.client,
                                stg.update_dt
                            FROM deaise.daar_stg_accounts stg
                            INNER JOIN deaise.daar_dwh_dim_accounts_hist tgt
                                ON stg.account_num = tgt.account_num
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            WHERE ( 1=0
                                    OR stg.valid_to <> tgt.valid_to OR (stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL) OR (stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL)
                                    OR stg.client <> tgt.client OR (stg.client IS NULL AND tgt.client IS NOT NULL) OR (stg.client IS NOT NULL AND tgt.client IS NULL) )
                                OR tgt.deleted_flg = 'Y' ) tmp
                        WHERE tgt.account_num = tmp.account_num 
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND ( tmp.valid_to <> tgt.valid_to OR (tmp.valid_to IS NULL AND tgt.valid_to IS NOT NULL) OR (tmp.valid_to IS NOT NULL AND tgt.valid_to IS NULL)
                                OR tmp.client <> tgt.client OR (tmp.client IS NULL AND tgt.client IS NOT NULL) OR (tmp.client IS NOT NULL AND tgt.client IS NULL) 
                                OR tgt.deleted_flg = 'Y')""")

## -- dwh_dim_clients --                
print('5.4 dwh_dim_clients_hist ') 
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone, 
                            stg.update_dt effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'N' deleted_flg
                        FROM deaise.daar_stg_clients stg
                        INNER JOIN deaise.daar_dwh_dim_clients_hist tgt
                            ON stg.client_id = tgt.client_id
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                        WHERE (1=0
                                OR stg.last_name <> tgt.last_name OR (stg.last_name IS NULL AND tgt.last_name IS NOT NULL) OR (stg.last_name IS NOT NULL AND tgt.last_name IS NULL)
                                OR stg.first_name <> tgt.first_name OR (stg.first_name IS NULL AND tgt.first_name IS NOT NULL) OR (stg.first_name IS NOT NULL AND tgt.first_name IS NULL)
                                OR stg.patronymic <> tgt.patronymic OR (stg.patronymic IS NULL AND tgt.patronymic IS NOT NULL) OR (stg.patronymic IS NOT NULL AND tgt.patronymic IS NULL)
                                OR stg.date_of_birth <> tgt.date_of_birth OR (stg.date_of_birth IS NULL AND tgt.date_of_birth IS NOT NULL) OR (stg.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL)
                                OR stg.passport_num <> tgt.passport_num OR (stg.passport_num IS NULL AND tgt.passport_num IS NOT NULL) OR (stg.passport_num IS NOT NULL AND tgt.passport_num IS NULL)
                                OR stg.passport_valid_to <> tgt.passport_valid_to OR (stg.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT NULL) OR (stg.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL)
                                OR stg.phone <>  tgt.phone OR (stg.phone IS NULL AND tgt.phone IS NOT NULL) OR (stg.phone IS NOT NULL AND tgt.phone IS NULL) )
                            OR tgt.deleted_flg = 'Y' """)
          
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_clients_hist tgt
                           SET
                            effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                            SELECT 
                                tgt.client_id,
                                stg.last_name,
                                stg.first_name,
                                stg.patronymic,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,
                                stg.update_dt
                            FROM deaise.daar_stg_clients stg
                            INNER JOIN deaise.daar_dwh_dim_clients_hist tgt
                                ON stg.client_id = tgt.client_id
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            WHERE ( 1=0
                                    OR stg.last_name <> tgt.last_name OR (stg.last_name IS NULL AND tgt.last_name IS NOT NULL) OR (stg.last_name IS NOT NULL AND tgt.last_name IS NULL)
                                    OR stg.first_name <> tgt.first_name OR (stg.first_name IS NULL AND tgt.first_name IS NOT NULL) OR (stg.first_name IS NOT NULL AND tgt.first_name IS NULL)
                                    OR stg.patronymic <> tgt.patronymic OR (stg.patronymic IS NULL AND tgt.patronymic IS NOT NULL) OR (stg.patronymic IS NOT NULL AND tgt.patronymic IS NULL)
                                    OR stg.date_of_birth <> tgt.date_of_birth OR (stg.date_of_birth IS NULL AND tgt.date_of_birth IS NOT NULL) OR (stg.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL)
                                    OR stg.passport_num <> tgt.passport_num OR (stg.passport_num IS NULL AND tgt.passport_num IS NOT NULL) OR (stg.passport_num IS NOT NULL AND tgt.passport_num IS NULL)
                                    OR stg.passport_valid_to <> tgt.passport_valid_to OR (stg.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT NULL) OR (stg.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL)
                                    OR stg.phone <>  tgt.phone OR (stg.phone IS NULL AND tgt.phone IS NOT NULL) OR (stg.phone IS NOT NULL AND tgt.phone IS NULL) ) 
                                OR tgt.deleted_flg = 'Y' ) tmp
                        WHERE tgt.client_id = tmp.client_id
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')                            
                            AND ( tmp.last_name <> tgt.last_name OR (tmp.last_name IS NULL AND tgt.last_name IS NOT NULL) OR (tmp.last_name IS NOT NULL AND tgt.last_name IS NULL)
                                OR tmp.first_name <> tgt.first_name OR (tmp.first_name IS NULL AND tgt.first_name IS NOT NULL) OR (tmp.first_name IS NOT NULL AND tgt.first_name IS NULL)
                                OR tmp.patronymic <> tgt.patronymic OR (tmp.patronymic IS NULL AND tgt.patronymic IS NOT NULL) OR (tmp.patronymic IS NOT NULL AND tgt.patronymic IS NULL)
                                OR tmp.date_of_birth <> tgt.date_of_birth OR (tmp.date_of_birth IS NULL AND tgt.date_of_birth IS NOT NULL) OR (tmp.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL)
                                OR tmp.passport_num <> tgt.passport_num OR (tmp.passport_num IS NULL AND tgt.passport_num IS NOT NULL) OR (tmp.passport_num IS NOT NULL AND tgt.passport_num IS NULL)
                                OR tmp.passport_valid_to <> tgt.passport_valid_to OR (tmp.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT NULL) OR (tmp.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL)
                                OR tmp.phone <>  tgt.phone OR (tmp.phone IS NULL AND tgt.phone IS NOT NULL) OR (tmp.phone IS NOT NULL AND tgt.phone IS NULL) 
                                OR tgt.deleted_flg = 'Y' )""")

## ## << Удаление данных в детальном слое DDS (формат SCD2)>>  
print('6. Удаление в DDS')  

## -- dwh_dim_terminals -- 
print('6.1 dwh_dim_terminals_hist ')
cursor_dwh.execute( """INSERT INTO deaise.daar_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            tgt.terminal_id, 
                            tgt.terminal_type,
                            tgt.terminal_city,
                            tgt.terminal_address,
                            CAST(now() AS DATE) effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'Y' deleted_flg 
                        FROM deaise.daar_dwh_dim_terminals_hist tgt
                        LEFT JOIN deaise.daar_meta_deleted stg
                            ON stg.terminal_id = tgt.terminal_id
                        WHERE stg.terminal_id IS NULL
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND tgt.deleted_flg = 'N' """)

cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_terminals_hist tgt
                            SET effective_to = now()- interval '1 day'
                        WHERE tgt.terminal_id IN (
                            SELECT 
                                tgt.terminal_id
                            FROM deaise.daar_dwh_dim_terminals_hist tgt
                            LEFT JOIN deaise.daar_meta_deleted stg
                                ON stg.terminal_id = tgt.terminal_id
                            WHERE stg.terminal_id IS NULL 
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                                AND tgt.deleted_flg = 'N')
                        AND tgt.deleted_flg = 'N' AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') """)

## -- dwh_dim_cards -- 
print('6.2 dwh_dim_cards_hist ')
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT
                            tgt.card_num,
                            tgt.account_num,
                            CAST(now() AS DATE) effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'Y' deleted_flg
                        FROM deaise.daar_dwh_dim_cards_hist tgt
                        LEFT JOIN deaise.daar_meta_deleted stg
                            ON stg.card_num = tgt.card_num
                        WHERE stg.card_num IS NULL
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND tgt.deleted_flg = 'N' """)
    
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_cards_hist tgt
                            SET effective_to = now()- interval '1 day'
                        WHERE tgt.card_num IN (
                            SELECT 
                                tgt.card_num
                            FROM deaise.daar_dwh_dim_cards_hist tgt
                            LEFT JOIN deaise.daar_meta_deleted stg
                               ON stg.card_num = tgt.card_num
                            WHERE stg.card_num IS NULL
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                                AND tgt.deleted_flg = 'N') 
                        AND tgt.deleted_flg = 'N' AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') """)

## -- dwh_dim_accounts -- 
print('6.3 dwh_dim_accounts_hist ')
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            tgt.account_num,
                            tgt.valid_to,
                            tgt.client,
                            CAST(now() AS DATE) effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'Y' deleted_flg 
                        FROM deaise.daar_dwh_dim_accounts_hist tgt
                        LEFT JOIN deaise.daar_meta_deleted stg
                            ON stg.account_num = tgt.account_num
                        WHERE stg.account_num IS NULL
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND tgt.deleted_flg = 'N' """)
    
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_accounts_hist tgt
                            SET effective_to = now()- interval '1 day'
                        WHERE tgt.account_num IN (
                            SELECT 
                                tgt.account_num
                            FROM deaise.daar_dwh_dim_accounts_hist tgt
                            LEFT JOIN deaise.daar_meta_deleted stg
                                ON stg.account_num = tgt.account_num
                            WHERE stg.account_num IS NULL
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                                AND tgt.deleted_flg = 'N') 
                        AND tgt.deleted_flg = 'N' AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')""")

## -- dwh_dim_clients -- 
print('6.4 dwh_dim_clients_hist ')
cursor_dwh.execute( """ INSERT INTO deaise.daar_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg)
                        SELECT 
                            tgt.client_id,
                            tgt.last_name,
                            tgt.first_name,
                            tgt.patronymic,
                            tgt.date_of_birth,
                            tgt.passport_num,
                            tgt.passport_valid_to,
                            tgt.phone,
                            CAST(now() AS DATE) effective_from,
                            TO_DATE('9999-12-31', 'YYYY-MM-DD') effective_to,
                            'Y' deleted_flg 
                        FROM deaise.daar_dwh_dim_clients_hist tgt
                        LEFT JOIN deaise.daar_meta_deleted stg
                            ON stg.client_id = tgt.client_id
                        WHERE stg.client_id IS NULL
                            AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND tgt.deleted_flg = 'N' """)
    
cursor_dwh.execute( """ UPDATE deaise.daar_dwh_dim_clients_hist tgt
                            SET effective_to = now()- interval '1 day'
                        WHERE tgt.client_id IN (
                            SELECT 
                                tgt.client_id
                            FROM deaise.daar_dwh_dim_clients_hist tgt
                            LEFT JOIN deaise.daar_meta_deleted stg
                               ON stg.client_id = tgt.client_id
                            WHERE stg.client_id IS NULL
                                AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD')
                                AND tgt.deleted_flg = 'N') 
                        AND tgt.deleted_flg = 'N' AND tgt.effective_to = TO_DATE('9999-12-31','YYYY-MM-DD') """)


## ## << Обновление метаданных maxdate>>
print('7. Обновление метаданных ') 

## -- stg_terminals >> meta_maxdate -- 
print('7.1 stg_terminals')
cursor_dwh.execute( """UPDATE deaise.daar_meta_maxdate
                        SET max_update_dt = coalesce((SELECT max(update_dt) FROM deaise.daar_stg_terminals), max_update_dt)
                       WHERE schema_name = 'deaise' AND table_name = 'daar_stg_terminals' """)
                       
## -- stg_cards >> meta_maxdate --                        
print('7.2 stg_cards')
cursor_dwh.execute( """UPDATE deaise.daar_meta_maxdate
                        SET max_create_dt = coalesce((SELECT max(create_dt) FROM deaise.daar_stg_cards), max_create_dt),
                            max_update_dt = coalesce((SELECT max(update_dt) FROM deaise.daar_stg_cards), max_update_dt)
                       WHERE schema_name = 'deaise' AND table_name = 'daar_stg_cards' """)

## -- stg_accounts >> meta_maxdate --                         
print('7.3 stg_accounts')
cursor_dwh.execute( """UPDATE deaise.daar_meta_maxdate
                        SET max_create_dt = coalesce((SELECT max(create_dt) FROM deaise.daar_stg_accounts), max_create_dt),
                            max_update_dt = coalesce((SELECT max(update_dt) FROM deaise.daar_stg_accounts), max_update_dt)
                       WHERE schema_name = 'deaise' AND table_name = 'daar_stg_accounts' """)

## -- stg_clients >> meta_maxdate --   
print('7.4 stg_clients')
cursor_dwh.execute( """UPDATE deaise.daar_meta_maxdate
                        SET max_create_dt = coalesce((SELECT max(create_dt) FROM deaise.daar_stg_clients), max_create_dt),
                            max_update_dt = coalesce((SELECT max(update_dt) FROM deaise.daar_stg_clients), max_update_dt)
                       WHERE schema_name = 'deaise' AND table_name = 'daar_stg_clients' """)


## ## << Переименование и перемещение файлов-источников в архив>>
print('8. Переименование и перемещение файлов-источников в архив')

print('8.1 запуск скрипта "backup_files.py')
result = subprocess.run(["/home/project/py_scripts/backup_files.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
if result.returncode == 0:
    print("Скрипт успешно выполнен")
else:
    print("Скрипт завершился с ошибкой:")
    print(result.stderr)  


## ## << Построение отчета  >>
print('9. Загрузка в витрины')
## rep_fraud
print('9.1. rep_fraud')                                 
cursor_dwh.execute( """ INSERT INTO deaise.daar_rep_fraud(
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt)
                        WITH clients_tab AS (
                                    SELECT 
                                        tr.trans_date,
                                        cl.passport_num passport, 
                                        (cl.last_name ||' ' || cl.first_name ||' '|| cl.patronymic) fio, 
                                        cl.phone,
                                        CAST(now() AS date) report_dt,
                                        cl.passport_valid_to,
                                        cl.client_id,
                                        ca.card_num,
                                        ac.valid_to ac_valid_to,
                                        tr.trans_id,
                                        tr.oper_type,
                                        tr.amt,
                                        tr.oper_result,
                                        te.terminal_id,
                                        te.terminal_city	
                                    FROM deaise.daar_dwh_dim_clients_hist cl
                                    LEFT JOIN deaise.daar_dwh_dim_accounts_hist ac
                                        ON ac.client = cl.client_id
                                    LEFT JOIN deaise.daar_dwh_dim_cards_hist ca
                                        ON ca.account_num = ac.account_num
                                    LEFT JOIN deaise.daar_dwh_fact_transactions tr
                                        ON tr.card_num = ca.card_num 
                                    LEFT JOIN deaise.daar_dwh_dim_terminals_hist te
                                        ON te.terminal_id = tr.terminal),
                            tab_for_3_4 AS (
                                    SELECT 
                                        passport,
                                        fio,
                                        phone,
                                        report_dt,
                                        terminal_city,
                                        LEAD (terminal_city ) OVER (partition by passport,fio,phone,card_num order by trans_date) terminal_city_lead,
                                        LEAD (trans_date ) OVER (partition by passport,fio,phone,card_num order by trans_date) trans_date_lead,
                                        card_num,
                                        LAG (oper_result, 3) OVER (partition by passport,fio,phone,card_num order by trans_date) oper_result_lag_3,
                                        LAG (oper_result, 2) OVER (partition by passport,fio,phone,card_num order by trans_date) oper_result_lag_2,
                                        LAG (oper_result, 1) OVER (partition by passport,fio,phone,card_num order by trans_date) oper_result_lag_1,
                                        oper_result,
                                        LAG (amt, 3) OVER (partition by passport,fio,phone,card_num order by trans_date) amt_lag_3,
                                        LAG (amt, 2) OVER (partition by passport,fio,phone,card_num order by trans_date) amt_lag_2,
                                        LAG (amt, 1) OVER (partition by passport,fio,phone,card_num order by trans_date) amt_lag_1,
                                        amt,
                                        LAG (trans_date, 3) OVER (partition by passport,fio,phone,card_num order by trans_date) trans_date_lag_3,
                                        trans_date
                                    FROM clients_tab cl)	
                        SELECT 
                            stg.event_dt,
                            stg.passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        FROM (
                            SELECT 
                                trans_date event_dt,
                                passport,
                                fio,
                                phone,
                                (1) event_type,
                                report_dt
                            FROM clients_tab cl
                            WHERE cast(cl.trans_date AS date) > cl.passport_valid_to 
                                OR cl.passport IN (SELECT passport_num 
                                                   FROM deaise.daar_dwh_fact_passport_blacklist
                                                   WHERE entry_dt <= cast(cl.trans_date AS date)) 
                            UNION ALL
                            SELECT 
                                trans_date,
                                passport,
                                fio,
                                phone,
                                (2) event_type,
                                report_dt
                            FROM clients_tab cl
                            WHERE cast(cl.trans_date AS date) > cl.ac_valid_to    
                            UNION ALL
                            SELECT  
                                max(cl.trans_date),
                                cl.passport,
                                cl.fio,
                                cl.phone,
                                (3) event_type,
                                max(cl.report_dt)
                            FROM clients_tab cl   
                            INNER JOIN tab_for_3_4 t
                                ON cl.terminal_city<>terminal_city_lead
                                AND cl.card_num = t.card_num
                                AND cl.trans_date=t.trans_date	
                            WHERE oper_type <> 'DEPOSIT'
                                AND (trans_date_lead - cl.trans_date ) <= CAST('01:00:00' AS time)
                            GROUP BY cl.passport, cl.fio, cl.phone
                            UNION ALL 
                            SELECT 
                                trans_date,
                                passport,
                                fio,
                                phone,
                                (4) event_type,
                                report_dt
                            FROM tab_for_3_4
                            WHERE oper_result_lag_3 = 'REJECT' 
                                AND oper_result_lag_2 = 'REJECT' 
                                AND oper_result_lag_1 = 'REJECT'
                                AND oper_result = 'SUCCESS'
                                AND amt_lag_3 > amt_lag_2
                                AND amt_lag_2 > amt_lag_1
                                AND amt_lag_1 > amt
                                AND (trans_date - trans_date_lag_3) <= CAST('00:20:00' AS time)
                            ) stg  
                        LEFT JOIN deaise.daar_rep_fraud tgt
                            ON 1=1
                            AND stg.passport = tgt.passport
                            AND stg.report_dt = tgt.report_dt
                            AND stg.event_dt = tgt.event_dt
                        WHERE tgt.passport IS NULL""")  


#####    #####    -----------------------------------   #####    #####  


## ## << Закрываем соединение >>
conn_dwh.commit()
print('99. Закрытие подключений')
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()

## - создание файла flag_for_meta_maxdate.txt, чтобы не допустить повторного выполнения кода с первичным заполнением meta_maxdate
with open("/home/project/py_scripts/flag_for_meta_maxdate.txt", "w") as f:
        f.write("Flag file created to indicate that the code has been executed.")
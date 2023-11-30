
----------REP----------
CREATE TABLE IF NOT EXISTS deaise.daar_rep_fraud (
	event_dt TIMESTAMP(0),
	passport VARCHAR(15),
	fio VARCHAR(60),
	phone VARCHAR(16),
	event_type VARCHAR(15),
	report_dt DATE
);

----------FACT----------
CREATE TABLE IF NOT EXISTS deaise.daar_dwh_fact_passport_blacklist (
	passport_num VARCHAR(30),
	entry_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.daar_dwh_fact_transactions (
	trans_id VARCHAR(15),
	trans_date TIMESTAMP,  
	card_num CHAR(20),       
	oper_type VARCHAR(15),
	amt DECIMAL(9,2),
	oper_result VARCHAR(15),
	terminal VARCHAR(10)
);

----------DIM----------
CREATE TABLE IF NOT EXISTS deaise.daar_dwh_dim_terminals_hist (
	terminal_id VARCHAR(10),
	terminal_type VARCHAR(10),
	terminal_city VARCHAR(20),
	terminal_address VARCHAR(100),
	effective_from DATE,
	effective_to DATE,
	deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS deaise.daar_dwh_dim_cards_hist (
	card_num CHAR(20),  
	account_num CHAR(20), 
	effective_from DATE,
	effective_to DATE,
	deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS deaise.daar_dwh_dim_accounts_hist (
	account_num CHAR(20), 
	valid_to DATE,
	client VARCHAR(10),
	effective_from DATE,
	effective_to DATE,
	deleted_flg CHAR(1)
);

CREATE TABLE IF NOT EXISTS deaise.daar_dwh_dim_clients_hist (
	client_id VARCHAR(10),
	last_name VARCHAR(20),
	first_name VARCHAR(20),
	patronymic VARCHAR(20),
	date_of_birth DATE,
	passport_num VARCHAR(15),
	passport_valid_to DATE,
	phone VARCHAR(16),
	effective_from DATE,
	effective_to DATE,
	deleted_flg CHAR(1)
);

----------STG----------
CREATE TABLE IF NOT EXISTS deaise.daar_stg_terminals (
	terminal_id VARCHAR(10),
	terminal_type VARCHAR(10),
	terminal_city VARCHAR(20),
	terminal_address VARCHAR(100),
	update_dt DATE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deaise.daar_stg_cards (
	card_num CHAR(20),  
	account_num CHAR(20), 
	create_dt DATE,
	update_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.daar_stg_accounts (
	account_num CHAR(20),  
	valid_to DATE,
	client VARCHAR(10),
	create_dt DATE,
	update_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.daar_stg_clients (
	client_id VARCHAR(10),
	last_name VARCHAR(20),
	first_name VARCHAR(20),
	patronymic VARCHAR(20),
	date_of_birth DATE,
	passport_num VARCHAR(15),
	passport_valid_to DATE,
	phone VARCHAR(16),
	create_dt DATE,
	update_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.daar_stg_passport_blacklist (
	passport_num VARCHAR(30),
	entry_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.daar_stg_transactions (
	trans_id VARCHAR(15),
	trans_date TIMESTAMP,
	card_num CHAR(20),       
	oper_type VARCHAR(15),
	amt DECIMAL(9,2),
	oper_result VARCHAR(15),
	terminal VARCHAR(10)
);
----- * --- Дополнительные таблицы --- * -----

CREATE TABLE IF NOT EXISTS deaise.daar_meta_maxdate (
	schema_name VARCHAR(30),
	table_name VARCHAR(30),
	max_create_dt DATE,
	max_update_dt DATE
);

CREATE TABLE IF NOT EXISTS  deaise.daar_meta_deleted( 
	terminal_id VARCHAR(10),
	card_num CHAR(20),
	account_num CHAR(20),
	client_id VARCHAR(10)
);
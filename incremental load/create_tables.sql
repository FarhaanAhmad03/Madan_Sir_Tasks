create table src_customer(
cust_id INT, 
cust_name VARCHAR(250),
cust_phone VARCHAR(50),
created_date DATE,
modified_date DATE
)

create table stg_customer(
cust_id INT, 
cust_name VARCHAR(250),
cust_phone VARCHAR(50),
created_date DATE,
modified_date DATE
)

create table control_table(
table_id VARCHAR(50),
max_created_date DATE,
max_modified_date DATE
)

create table dim_customer(
cust_id INT PRIMARY KEY, 
cust_name VARCHAR(250),
cust_phone VARCHAR(50),
created_date DATE,
modified_date DATE
)


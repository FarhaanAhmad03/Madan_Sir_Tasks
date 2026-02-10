update control_table
SET 
    max_created_date = (SELECT max(created_date) from stg_customer),
    max_modified_date = (SELECT max(modified_date) from stg_customer)
where table_id = "src_customer";
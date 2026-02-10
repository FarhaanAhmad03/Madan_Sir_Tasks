insert into stg_customer 
select * from src_customer
WHERE created_date > (SELECT max_created_date from control_table)
OR 
      modified_date > (SELECT max_modified_date from control_table)
      
      

      

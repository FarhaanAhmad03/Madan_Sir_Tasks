UPDATE dim_customer d
JOIN stg_customer s
On 
d.cust_id = s.cust_id
SET     
    d.cust_name = s.cust_name,
    d.cust_phone = s.cust_phone,
    d.created_date = s.created_date,
    d.modified_date = s.modified_date;
    

INSERT into dim_customer 
SELECT s.*
FROM stg_customer s
LEFT JOIN dim_customer d
ON 
d.cust_id = s.cust_id
WHERE
d.cust_id IS NULL
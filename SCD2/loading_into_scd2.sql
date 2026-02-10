# first time load
INSERT INTO scd2_cust_dim (
    cust_id,
    phone_no,
    start_dt,
    end_dt,
    created_dt,
    modified_dt,
    is_active,
    load_dt
)
SELECT
    s.cust_id,
    s.phone_no,
    s.modified_dt,
    '9999-12-31',
    s.created_dt,
    s.modified_dt,
    1,
    NOW()
FROM cust_src s;


#Updating Old Records
UPDATE scd2_cust_dim d
JOIN cust_src s
  ON d.cust_id = s.cust_id
SET
    d.end_dt = s.modified_dt,
    d.is_active = 0,
    d.modified_dt = NOW()
WHERE d.is_active = 1
  AND d.phone_no <> s.phone_no;

#Insert New Records
INSERT INTO scd2_cust_dim (
    cust_id,
    phone_no,
    start_dt,
    end_dt,
    created_dt,
    modified_dt,
    is_active,
    load_dt
)
SELECT
    s.cust_id,
    s.phone_no,
    s.modified_dt,
    '9999-12-31',
    s.created_dt,
    s.modified_dt,
    1,
    NOW()
FROM cust_src s
LEFT JOIN scd2_cust_dim d
  ON s.cust_id = d.cust_id
 AND d.is_active = 1
WHERE d.cust_id IS NULL
   OR d.phone_no <> s.phone_no;
   
SELECT cust_key, cust_id, phone_no, start_dt, end_dt, is_active
FROM scd2_cust_dim
ORDER BY cust_id, start_dt;



select * from scd2_cust_dim order by cust_id

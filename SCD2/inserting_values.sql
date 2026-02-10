INSERT INTO cust_src (cust_id, phone_no, created_dt, modified_dt)
VALUES
(1001, '9xx-1111', NOW(),NOW()),
(1002, '9xx-2222', NOW(),NOW()),
(1003, '9xx-3333', NOW(),NOW());


UPDATE cust_src
SET phone_no = '9xx-9999',
    modified_dt = '2024-12-21 09:00:00'
WHERE cust_id = 1001;

UPDATE cust_src
SET phone_no = '9xx-2026',
    modified_dt = '2025-12-26 09:00:00'
WHERE cust_id = 1002;

select * from cust_src

INSERT INTO cust_src (cust_id, phone_no, created_dt, modified_dt)
VALUES
(1006, '9xx-2222', NOW(),NOW())

UPDATE cust_src
SET phone_no = '9xx-8888',
    modified_dt = NOW()
WHERE cust_id = 1001;

#Inserting Values

INSERT INTO src_customer VALUES
(1,'Ravi', '9872346510', '2025-12-25', '2025-12-25'),
(2,'Mohit', '9864376511', '2025-12-25', '2025-12-25'),
(3,'Suresh', '9272646411', '2025-12-25', '2025-12-25'),
(4,'Rawish', '8877346613', '2025-12-25', '2025-12-25'),
(5,'Ashish', '6872356212', '2025-12-25', '2025-12-25')

INSERT INTO control_table VALUES
('src_customer', '1111-01-01','1111-01-01')


UPDATE src_customer
SET cust_phone = '99999934532',
    modified_date = CURDATE()
WHERE cust_id = 2;

UPDATE src_customer
SET cust_phone = '8888234698',
    modified_date = CURDATE()
WHERE cust_id = 4;



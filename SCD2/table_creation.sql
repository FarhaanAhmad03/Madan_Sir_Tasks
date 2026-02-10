CREATE TABLE cust_src
(
cust_id INT,
phone_no varchar(250),
created_dt TIMESTAMP,
modified_dt TIMESTAMP
)

DROP TABLE IF EXISTS scd2_cust_dim;

CREATE TABLE scd2_cust_dim (
    cust_key     BIGINT AUTO_INCREMENT PRIMARY KEY,
    cust_id      INT,
    phone_no     VARCHAR(15),
    start_dt     DATETIME,
    end_dt       DATETIME,
    created_dt  DATETIME,
    modified_dt DATETIME,
    is_active    TINYINT,   -- 1 = active, 0 = inactive
    load_dt      DATETIME
);

















drop table if exists customer;

CREATE TABLE customer
(
    id integer NOT NULL,
    fullname character varying(255),
    email character varying(255),
    CONSTRAINT customer_pkey PRIMARY KEY (id)
);

INSERT INTO customerdb.customer (id, fullname, email) VALUES (6, 'John Doe', 'jd@example.com');

UPDATE customerdb.customer t SET t.email = 'john.doe@example.com' WHERE t.id = 1;

DELETE FROM customerdb.customer WHERE id = 1;




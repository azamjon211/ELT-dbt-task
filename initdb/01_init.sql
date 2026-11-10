-- Manba (OLTP) va ombor (warehouse) bazalari
CREATE DATABASE shop;
CREATE DATABASE warehouse;

\connect shop

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT           NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    status       TEXT          NOT NULL DEFAULT 'paid',
    created_at   TIMESTAMP     NOT NULL
);

-- Oxirgi 5 kun uchun test ma'lumotlar (kecha ham, bugun ham bor)
INSERT INTO orders (customer_id, amount, status, created_at)
SELECT
    (random() * 50)::int + 1,
    round((random() * 200 + 5)::numeric, 2),
    (ARRAY['paid','paid','paid','shipped','cancelled'])[floor(random() * 5)::int + 1],
    (current_date - (d || ' day')::interval) + (random() * interval '23 hours')
FROM generate_series(0, 4) AS d,
     generate_series(1, 20) AS n;

\connect warehouse

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

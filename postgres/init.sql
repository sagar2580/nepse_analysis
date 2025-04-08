-- -- Create database and application user with limited privileges
-- DO $$
-- BEGIN
--     IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'nepse') THEN
--         CREATE DATABASE nepse;
--     END IF;
-- END
-- $$;

-- -- Create user with password (change to a strong password in production)
-- CREATE USER admin WITH PASSWORD 'admin';

-- -- Grant connection privilege
-- GRANT CONNECT ON DATABASE nepse TO admin;

-- -- Connect to our database
-- \c nepse

-- -- Drop table if it exists (corrected syntax)
-- DROP TABLE IF EXISTS nepse1;

-- -- Create tables with improved column definitions
-- CREATE TABLE IF NOT EXISTS nepse1 (
--     id SERIAL PRIMARY KEY,
--     transaction_no VARCHAR(50) UNIQUE,
--     symbol VARCHAR(20),
--     buyer VARCHAR(10),
--     seller VARCHAR(10),
--     quantity INTEGER,  -- Changed to integer as quantity is typically a number
--     rate DECIMAL(10,2),  -- Changed to decimal for monetary values
--     amount DECIMAL(12,2),  -- Changed to decimal for monetary values
--     scraped_at TIMESTAMP,
--     trade_date DATE,
--     page_number INTEGER,
--     kafka_offset BIGINT,
--     processed_at TIMESTAMP DEFAULT NOW(),
--     CONSTRAINT unique_offset UNIQUE(kafka_offset)
-- );

-- CREATE TABLE IF NOT EXISTS kafka_offsets (
--     topic VARCHAR(255),
--     partition INTEGER,
--     kafka_offset BIGINT,  -- Fixed column name consistency
--     PRIMARY KEY (topic, partition)
-- );

-- -- Grant necessary privileges to application user
-- GRANT USAGE ON SCHEMA public TO admin;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO admin;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO admin;

-- -- Test data insertion
-- INSERT INTO nepse1(symbol, transaction_no) VALUES('TEST', 'zyzzz');

-- -- Verify data
-- SELECT * FROM nepse1;

-- Check and create database if not exists (PostgreSQL 13 compatible)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'nepse') THEN
        CREATE DATABASE nepse;
    END IF;
END
$$;

-- Create role if not exists (PostgreSQL 9.5+ compatible)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
        CREATE USER admin WITH PASSWORD 'admin';
    ELSE
        ALTER USER admin WITH PASSWORD 'admin';  -- Ensure password is correct
    END IF;
END
$$;

-- Grant connection privilege (idempotent)
GRANT CONNECT ON DATABASE nepse TO admin;

-- Connect to our database
\c nepse

-- Create tables with IF NOT EXISTS
DROP TABLE IF EXISTS nepse1;

CREATE TABLE IF NOT EXISTS nepse1 (
    id SERIAL PRIMARY KEY,
    transaction_no VARCHAR(50) UNIQUE,
    symbol VARCHAR(20),
    buyer VARCHAR(10),
    seller VARCHAR(10),
    quantity VARCHAR(10),
    rate varchar(40),
    amount varchar(40),
    scraped_at TIMESTAMP,
    trade_date DATE,
    page_number INTEGER,
    kafka_offset BIGINT,
    processed_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_offset UNIQUE(kafka_offset)
);

CREATE TABLE IF NOT EXISTS kafka_offsets (
    topic VARCHAR(255),
    partition INTEGER,
    kafka_offset BIGINT,
    PRIMARY KEY (topic, partition)
);

-- Grant privileges (idempotent)
GRANT USAGE ON SCHEMA public TO admin;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO admin;

-- Test data insertion (only if not exists)
INSERT INTO nepse1 (symbol, transaction_no)
SELECT 'TEST', 'zyzzz'
WHERE NOT EXISTS (SELECT 1 FROM nepse1 WHERE transaction_no = 'zyzzz');
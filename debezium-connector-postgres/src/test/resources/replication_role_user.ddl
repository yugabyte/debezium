DROP ROLE IF EXISTS ybpgconn;

CREATE ROLE ybpgconn WITH LOGIN REPLICATION;
CREATE SCHEMA ybpgconn AUTHORIZATION ybpgconn;

GRANT CREATE ON DATABASE yugabyte TO ybpgconn;

BEGIN;
    CREATE OR REPLACE PROCEDURE ybpgconn.set_yb_read_time(value TEXT)
    LANGUAGE plpgsql
    AS $$
    BEGIN
      EXECUTE 'SET LOCAL yb_read_time = ' || quote_literal(value);
    END;
    $$
    SECURITY DEFINER;

    REVOKE EXECUTE ON PROCEDURE ybpgconn.set_yb_read_time FROM PUBLIC;
    GRANT EXECUTE ON PROCEDURE ybpgconn.set_yb_read_time TO ybpgconn;
COMMIT;
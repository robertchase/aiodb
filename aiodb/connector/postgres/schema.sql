---DROP DATABASE IF EXISTS "test_postgres";
---CREATE DATABASE "test_postgres";

---REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM test_postgres;
---REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM test_postgres;
---REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM test_postgres;
---REVOKE USAGE ON SCHEMA public FROM test_postgres;

---DROP ROLE "test_postgres";
---CREATE ROLE "test_postgres";

---GRANT "test_postgres" TO "test";

\c "test_postgres";

DROP TABLE IF EXISTS "test";
DROP TYPE IF EXISTS "disposition";

CREATE TYPE disposition AS ENUM ('good', 'bad', 'ugly');
CREATE TABLE "test" (
    "id" SERIAL,
    "a_sin" SMALLINT,
    "a_int" INTEGER,
    "a_bin" BIGINT,
    "e_num" NUMERIC,
    "e_nu2" NUMERIC(10,2),
    "f_rea" REAL,
    "f_dou" DOUBLE PRECISION,
    "g_sms" SMALLSERIAL,
    "g_ser" SERIAL,
    "g_bis" BIGSERIAL,
    "b" VARCHAR(100),
    "c" TIMESTAMP,
    "d" BOOLEAN,
    "f" DATE,
    "g" TIME,
    "h" disposition
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "test" TO "test_postgres";
GRANT USAGE ON SEQUENCE "test_id_seq" TO "test_postgres";
GRANT USAGE ON SEQUENCE "test_g_sms_seq" TO "test_postgres";
GRANT USAGE ON SEQUENCE "test_g_ser_seq" TO "test_postgres";
GRANT USAGE ON SEQUENCE "test_g_bis_seq" TO "test_postgres";

---DROP DATABASE IF EXISTS "test_postgres";
---CREATE DATABASE "test_postgres";

---REVOKE ALL ON DATABASE "test_postgres" FROM PUBLIC;

---CREATE ROLE "test_postgres";
---GRANT "test_postgres" TO "test";

---GRANT CONNECT ON DATABASE "test_postgres" TO "test_postgres";
GRANT USAGE ON SCHEMA "public" TO "test_postgres";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "public" TO "test_postgres";
GRANT USAGE ON ALL SEQUENCES IN SCHEMA "public" TO "test_postgres";

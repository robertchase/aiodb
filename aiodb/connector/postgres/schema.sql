DROP TABLE IF EXISTS "test";
CREATE TYPE disposition AS ENUM ('good', 'bad', 'ugly');
CREATE TABLE "test" (
    "a" INT,
    "b" VARCHAR(100),
    "c" TIMESTAMP,
    "d" BOOLEAN,
    "e" NUMERIC(10,2),
    "f" DATE,
    "g" TIME,
    "h" disposition
);
GRANT SELECT ON TABLE test TO foo;
INSERT INTO "test" VALUES(10, 'eleven', 'now()', '0', 123.45, 'now()', 'now()', 'bad');

DROP TABLE IF EXISTS "simple";
CREATE TABLE "simple" (
    "a" INT
);
INSERT INTO "simple" VALUES (1), (2), (3);
GRANT SELECT ON TABLE simple TO foo;

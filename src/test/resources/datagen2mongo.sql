CREATE TABLE data (
   name STRING,
   age INT
) WITH (
   'connector' = 'datagen'
);

CREATE TABLE mongo (
  name STRING,
  age INT
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://192.168.129.126:27017,192.168.129.125:27017,192.168.129.124:27017',
  'database' = 'test',
  'collection' = 'test'
);

INSERT INTO mongo SELECT * FROM data;
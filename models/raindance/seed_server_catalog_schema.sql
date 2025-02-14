MODEL (
  kind SEED (
    path '$root/seeds/raindance/server_catalog_schema.csv'
  ),
  columns (
    foretag_id varchar(max),
    server_ip_adress varchar(max),
    server_fqdn varchar(max),
    [catalog] varchar(max),
    [schema] varchar(max)
  )
);
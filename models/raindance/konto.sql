MODEL (
  kind SEED (
    path '$root/seeds/raindance/konto.csv'
  ),
  columns (
    foretag_id varchar(max),
    table varchar(max),
    source_column varchar(max),
    target_column varchar(max)
  ),
);
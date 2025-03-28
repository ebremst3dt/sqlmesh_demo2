
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRA01_GILTIG_FOM': 'varchar(max)', 'FRA01_GILTIG_TOM': 'varchar(max)', 'FRA01_ID': 'varchar(max)', 'FRA01_ID_TEXT': 'varchar(max)', 'FRA01_PASSIV': 'varchar(max)', 'FRA01_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
    ),
    cron="@daily"
)

        
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = f"""
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata292' as _source,
		CONVERT(varchar(max), FRA01_GILTIG_FOM, 126) AS FRA01_GILTIG_FOM,
		CONVERT(varchar(max), FRA01_GILTIG_TOM, 126) AS FRA01_GILTIG_TOM,
		CAST(FRA01_ID AS VARCHAR(MAX)) AS FRA01_ID,
		CAST(FRA01_ID_TEXT AS VARCHAR(MAX)) AS FRA01_ID_TEXT,
		CAST(FRA01_PASSIV AS VARCHAR(MAX)) AS FRA01_PASSIV,
		CAST(FRA01_TEXT AS VARCHAR(MAX)) AS FRA01_TEXT,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS KONTO_GILTIG_FOM,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS KONTO_GILTIG_TOM,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS KONTO_ID_TEXT,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS KONTO_PASSIV,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS KONTO_TEXT,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS TSIK_GILTIG_FOM,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS TSIK_GILTIG_TOM,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS TSIK_ID,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS TSIK_ID_TEXT,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS TSIK_PASSIV,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS TSIK_TEXT 
	FROM utdata.utdata292.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
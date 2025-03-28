
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKLASS_GILTIG_FOM': 'varchar(max)', 'KKLASS_GILTIG_TOM': 'varchar(max)', 'KKLASS_ID': 'varchar(max)', 'KKLASS_ID_TEXT': 'varchar(max)', 'KKLASS_PASSIV': 'varchar(max)', 'KKLASS_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata801' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS KGRUPP_GILTIG_FOM,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS KGRUPP_GILTIG_TOM,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS KGRUPP_ID,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS KGRUPP_ID_TEXT,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS KGRUPP_PASSIV,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS KGRUPP_TEXT,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS KKLASS_GILTIG_FOM,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS KKLASS_GILTIG_TOM,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS KKLASS_ID,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS KKLASS_ID_TEXT,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS KKLASS_PASSIV,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS KKLASS_TEXT,
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
	FROM utdata.utdata801.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
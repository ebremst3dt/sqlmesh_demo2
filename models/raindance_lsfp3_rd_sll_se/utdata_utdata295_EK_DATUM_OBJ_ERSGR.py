
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ERSGR_DATUM_FOM': 'varchar(max)', 'ERSGR_DATUM_TOM': 'varchar(max)', 'ERSGR_GILTIG_FOM': 'varchar(max)', 'ERSGR_GILTIG_TOM': 'varchar(max)', 'ERSGR_ID': 'varchar(max)', 'ERSGR_ID_TEXT': 'varchar(max)', 'ERSGR_PASSIV': 'varchar(max)', 'ERSGR_TEXT': 'varchar(max)'},
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
    query = """
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), ERSGR_DATUM_FOM, 126) AS ersgr_datum_fom,
		CONVERT(varchar(max), ERSGR_DATUM_TOM, 126) AS ersgr_datum_tom,
		CONVERT(varchar(max), ERSGR_GILTIG_FOM, 126) AS ersgr_giltig_fom,
		CONVERT(varchar(max), ERSGR_GILTIG_TOM, 126) AS ersgr_giltig_tom,
		CAST(ERSGR_ID AS VARCHAR(MAX)) AS ersgr_id,
		CAST(ERSGR_ID_TEXT AS VARCHAR(MAX)) AS ersgr_id_text,
		CAST(ERSGR_PASSIV AS VARCHAR(MAX)) AS ersgr_passiv,
		CAST(ERSGR_TEXT AS VARCHAR(MAX)) AS ersgr_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_ERSGR) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'BORAD_DATUM_FOM': 'varchar(max)', 'BORAD_DATUM_TOM': 'varchar(max)', 'BORAD_GILTIG_FOM': 'varchar(max)', 'BORAD_GILTIG_TOM': 'varchar(max)', 'BORAD_ID': 'varchar(max)', 'BORAD_ID_TEXT': 'varchar(max)', 'BORAD_PASSIV': 'varchar(max)', 'BORAD_TEXT': 'varchar(max)'},
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
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), BORAD_DATUM_FOM, 126) AS borad_datum_fom,
		CONVERT(varchar(max), BORAD_DATUM_TOM, 126) AS borad_datum_tom,
		CONVERT(varchar(max), BORAD_GILTIG_FOM, 126) AS borad_giltig_fom,
		CONVERT(varchar(max), BORAD_GILTIG_TOM, 126) AS borad_giltig_tom,
		CAST(BORAD_ID AS VARCHAR(MAX)) AS borad_id,
		CAST(BORAD_ID_TEXT AS VARCHAR(MAX)) AS borad_id_text,
		CAST(BORAD_PASSIV AS VARCHAR(MAX)) AS borad_passiv,
		CAST(BORAD_TEXT AS VARCHAR(MAX)) AS borad_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BORAD

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
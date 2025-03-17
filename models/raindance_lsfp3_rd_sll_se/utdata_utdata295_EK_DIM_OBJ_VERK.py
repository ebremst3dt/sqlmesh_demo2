
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'VERK_GILTIG_FOM': 'varchar(max)', 'VERK_GILTIG_TOM': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERK_ID_TEXT': 'varchar(max)', 'VERK_PASSIV': 'varchar(max)', 'VERK_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS verk_giltig_fom,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS verk_giltig_tom,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS verk_id_text,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS verk_passiv,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS verk_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text 
	FROM utdata.utdata295.EK_DIM_OBJ_VERK ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
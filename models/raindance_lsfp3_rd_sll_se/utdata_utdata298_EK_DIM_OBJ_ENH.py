
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVDEL_GILTIG_FOM': 'varchar(max)', 'AVDEL_GILTIG_TOM': 'varchar(max)', 'AVDEL_ID': 'varchar(max)', 'AVDEL_ID_TEXT': 'varchar(max)', 'AVDEL_PASSIV': 'varchar(max)', 'AVDEL_TEXT': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), AVDEL_GILTIG_FOM, 126) AS avdel_giltig_fom,
		CONVERT(varchar(max), AVDEL_GILTIG_TOM, 126) AS avdel_giltig_tom,
		CAST(AVDEL_ID AS VARCHAR(MAX)) AS avdel_id,
		CAST(AVDEL_ID_TEXT AS VARCHAR(MAX)) AS avdel_id_text,
		CAST(AVDEL_PASSIV AS VARCHAR(MAX)) AS avdel_passiv,
		CAST(AVDEL_TEXT AS VARCHAR(MAX)) AS avdel_text,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS enh_giltig_fom,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS enh_giltig_tom,
		CAST(ENH_ID AS VARCHAR(MAX)) AS enh_id,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS enh_id_text,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS enh_passiv,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS enh_text 
	FROM utdata.utdata298.EK_DIM_OBJ_ENH ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
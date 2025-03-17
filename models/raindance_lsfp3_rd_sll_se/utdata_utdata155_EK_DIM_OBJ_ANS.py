
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANS_GILTIG_FOM': 'varchar(max)', 'ANS_GILTIG_TOM': 'varchar(max)', 'ANS_ID': 'varchar(max)', 'ANS_ID_TEXT': 'varchar(max)', 'ANS_PASSIV': 'varchar(max)', 'ANS_TEXT': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata155' as _source,
		CONVERT(varchar(max), ANS_GILTIG_FOM, 126) AS ans_giltig_fom,
		CONVERT(varchar(max), ANS_GILTIG_TOM, 126) AS ans_giltig_tom,
		CAST(ANS_ID AS VARCHAR(MAX)) AS ans_id,
		CAST(ANS_ID_TEXT AS VARCHAR(MAX)) AS ans_id_text,
		CAST(ANS_PASSIV AS VARCHAR(MAX)) AS ans_passiv,
		CAST(ANS_TEXT AS VARCHAR(MAX)) AS ans_text,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS avd_giltig_fom,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS avd_giltig_tom,
		CAST(AVD_ID AS VARCHAR(MAX)) AS avd_id,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS avd_id_text,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS avd_passiv,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS avd_text,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS enh_giltig_fom,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS enh_giltig_tom,
		CAST(ENH_ID AS VARCHAR(MAX)) AS enh_id,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS enh_id_text,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS enh_passiv,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS enh_text 
	FROM utdata.utdata155.EK_DIM_OBJ_ANS ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
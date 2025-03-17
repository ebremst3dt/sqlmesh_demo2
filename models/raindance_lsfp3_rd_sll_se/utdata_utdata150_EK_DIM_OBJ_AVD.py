
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'RAM_GILTIG_FOM': 'varchar(max)', 'RAM_GILTIG_TOM': 'varchar(max)', 'RAM_ID': 'varchar(max)', 'RAM_ID_TEXT': 'varchar(max)', 'RAM_PASSIV': 'varchar(max)', 'RAM_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata150' as _source,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS avd_giltig_fom,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS avd_giltig_tom,
		CAST(AVD_ID AS VARCHAR(MAX)) AS avd_id,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS avd_id_text,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS avd_passiv,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS avd_text,
		CONVERT(varchar(max), RAM_GILTIG_FOM, 126) AS ram_giltig_fom,
		CONVERT(varchar(max), RAM_GILTIG_TOM, 126) AS ram_giltig_tom,
		CAST(RAM_ID AS VARCHAR(MAX)) AS ram_id,
		CAST(RAM_ID_TEXT AS VARCHAR(MAX)) AS ram_id_text,
		CAST(RAM_PASSIV AS VARCHAR(MAX)) AS ram_passiv,
		CAST(RAM_TEXT AS VARCHAR(MAX)) AS ram_text 
	FROM utdata.utdata150.EK_DIM_OBJ_AVD ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
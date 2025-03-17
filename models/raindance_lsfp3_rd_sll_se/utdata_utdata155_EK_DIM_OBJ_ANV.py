
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANV_GILTIG_FOM': 'varchar(max)', 'ANV_GILTIG_TOM': 'varchar(max)', 'ANV_ID': 'varchar(max)', 'ANV_ID_TEXT': 'varchar(max)', 'ANV_PASSIV': 'varchar(max)', 'ANV_TEXT': 'varchar(max)', 'GRUPP_GILTIG_FOM': 'varchar(max)', 'GRUPP_GILTIG_TOM': 'varchar(max)', 'GRUPP_ID': 'varchar(max)', 'GRUPP_ID_TEXT': 'varchar(max)', 'GRUPP_PASSIV': 'varchar(max)', 'GRUPP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ANV_GILTIG_FOM, 126) AS anv_giltig_fom,
		CONVERT(varchar(max), ANV_GILTIG_TOM, 126) AS anv_giltig_tom,
		CAST(ANV_ID AS VARCHAR(MAX)) AS anv_id,
		CAST(ANV_ID_TEXT AS VARCHAR(MAX)) AS anv_id_text,
		CAST(ANV_PASSIV AS VARCHAR(MAX)) AS anv_passiv,
		CAST(ANV_TEXT AS VARCHAR(MAX)) AS anv_text,
		CONVERT(varchar(max), GRUPP_GILTIG_FOM, 126) AS grupp_giltig_fom,
		CONVERT(varchar(max), GRUPP_GILTIG_TOM, 126) AS grupp_giltig_tom,
		CAST(GRUPP_ID AS VARCHAR(MAX)) AS grupp_id,
		CAST(GRUPP_ID_TEXT AS VARCHAR(MAX)) AS grupp_id_text,
		CAST(GRUPP_PASSIV AS VARCHAR(MAX)) AS grupp_passiv,
		CAST(GRUPP_TEXT AS VARCHAR(MAX)) AS grupp_text 
	FROM utdata.utdata155.EK_DIM_OBJ_ANV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
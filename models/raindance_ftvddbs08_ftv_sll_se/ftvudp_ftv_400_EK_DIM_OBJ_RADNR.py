
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'RADNR_GILTIG_FOM': 'varchar(max)', 'RADNR_GILTIG_TOM': 'varchar(max)', 'RADNR_ID': 'varchar(max)', 'RADNR_ID_TEXT': 'varchar(max)', 'RADNR_PASSIV': 'varchar(max)', 'RADNR_TEXT': 'varchar(max)'},
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
		'ftvddbs08_ftv_sll_se_ftvudp_ftv_400' as _source,
		CONVERT(varchar(max), RADNR_GILTIG_FOM, 126) AS radnr_giltig_fom,
		CONVERT(varchar(max), RADNR_GILTIG_TOM, 126) AS radnr_giltig_tom,
		CAST(RADNR_ID AS VARCHAR(MAX)) AS radnr_id,
		CAST(RADNR_ID_TEXT AS VARCHAR(MAX)) AS radnr_id_text,
		CAST(RADNR_PASSIV AS VARCHAR(MAX)) AS radnr_passiv,
		CAST(RADNR_TEXT AS VARCHAR(MAX)) AS radnr_text 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_RADNR ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    
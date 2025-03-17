
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVT_GILTIG_FOM': 'varchar(max)', 'AVT_GILTIG_TOM': 'varchar(max)', 'AVT_ID': 'varchar(max)', 'AVT_ID_TEXT': 'varchar(max)', 'AVT_PASSIV': 'varchar(max)', 'AVT_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), AVT_GILTIG_FOM, 126) AS avt_giltig_fom,
		CONVERT(varchar(max), AVT_GILTIG_TOM, 126) AS avt_giltig_tom,
		CAST(AVT_ID AS VARCHAR(MAX)) AS avt_id,
		CAST(AVT_ID_TEXT AS VARCHAR(MAX)) AS avt_id_text,
		CAST(AVT_PASSIV AS VARCHAR(MAX)) AS avt_passiv,
		CAST(AVT_TEXT AS VARCHAR(MAX)) AS avt_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_AVT ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'VAL_GILTIG_FOM': 'varchar(max)', 'VAL_GILTIG_TOM': 'varchar(max)', 'VAL_ID': 'varchar(max)', 'VAL_ID_TEXT': 'varchar(max)', 'VAL_PASSIV': 'varchar(max)', 'VAL_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), VAL_GILTIG_FOM, 126) AS val_giltig_fom,
		CONVERT(varchar(max), VAL_GILTIG_TOM, 126) AS val_giltig_tom,
		CAST(VAL_ID AS VARCHAR(MAX)) AS val_id,
		CAST(VAL_ID_TEXT AS VARCHAR(MAX)) AS val_id_text,
		CAST(VAL_PASSIV AS VARCHAR(MAX)) AS val_passiv,
		CAST(VAL_TEXT AS VARCHAR(MAX)) AS val_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_VAL ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
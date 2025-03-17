
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRG_GILTIG_FOM': 'varchar(max)', 'FRG_GILTIG_TOM': 'varchar(max)', 'FRG_ID': 'varchar(max)', 'FRG_ID_TEXT': 'varchar(max)', 'FRG_PASSIV': 'varchar(max)', 'FRG_TEXT': 'varchar(max)', 'FRK_GILTIG_FOM': 'varchar(max)', 'FRK_GILTIG_TOM': 'varchar(max)', 'FRK_ID': 'varchar(max)', 'FRK_ID_TEXT': 'varchar(max)', 'FRK_PASSIV': 'varchar(max)', 'FRK_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), FRG_GILTIG_FOM, 126) AS frg_giltig_fom,
		CONVERT(varchar(max), FRG_GILTIG_TOM, 126) AS frg_giltig_tom,
		CAST(FRG_ID AS VARCHAR(MAX)) AS frg_id,
		CAST(FRG_ID_TEXT AS VARCHAR(MAX)) AS frg_id_text,
		CAST(FRG_PASSIV AS VARCHAR(MAX)) AS frg_passiv,
		CAST(FRG_TEXT AS VARCHAR(MAX)) AS frg_text,
		CONVERT(varchar(max), FRK_GILTIG_FOM, 126) AS frk_giltig_fom,
		CONVERT(varchar(max), FRK_GILTIG_TOM, 126) AS frk_giltig_tom,
		CAST(FRK_ID AS VARCHAR(MAX)) AS frk_id,
		CAST(FRK_ID_TEXT AS VARCHAR(MAX)) AS frk_id_text,
		CAST(FRK_PASSIV AS VARCHAR(MAX)) AS frk_passiv,
		CAST(FRK_TEXT AS VARCHAR(MAX)) AS frk_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_FRK ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
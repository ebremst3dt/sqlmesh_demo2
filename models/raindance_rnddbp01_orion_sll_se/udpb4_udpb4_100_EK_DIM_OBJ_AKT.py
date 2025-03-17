
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AKT_GILTIG_FOM': 'varchar(max)', 'AKT_GILTIG_TOM': 'varchar(max)', 'AKT_ID': 'varchar(max)', 'AKT_ID_TEXT': 'varchar(max)', 'AKT_PASSIV': 'varchar(max)', 'AKT_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), AKT_GILTIG_FOM, 126) AS akt_giltig_fom,
		CONVERT(varchar(max), AKT_GILTIG_TOM, 126) AS akt_giltig_tom,
		CAST(AKT_ID AS VARCHAR(MAX)) AS akt_id,
		CAST(AKT_ID_TEXT AS VARCHAR(MAX)) AS akt_id_text,
		CAST(AKT_PASSIV AS VARCHAR(MAX)) AS akt_passiv,
		CAST(AKT_TEXT AS VARCHAR(MAX)) AS akt_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_AKT ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
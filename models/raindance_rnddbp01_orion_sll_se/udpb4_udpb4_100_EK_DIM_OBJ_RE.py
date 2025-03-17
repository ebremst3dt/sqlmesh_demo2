
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'RE_GILTIG_FOM': 'varchar(max)', 'RE_GILTIG_TOM': 'varchar(max)', 'RE_ID': 'varchar(max)', 'RE_ID_TEXT': 'varchar(max)', 'RE_PASSIV': 'varchar(max)', 'RE_TEXT': 'varchar(max)', 'SA_GILTIG_FOM': 'varchar(max)', 'SA_GILTIG_TOM': 'varchar(max)', 'SA_ID': 'varchar(max)', 'SA_ID_TEXT': 'varchar(max)', 'SA_PASSIV': 'varchar(max)', 'SA_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), RE_GILTIG_FOM, 126) AS re_giltig_fom,
		CONVERT(varchar(max), RE_GILTIG_TOM, 126) AS re_giltig_tom,
		CAST(RE_ID AS VARCHAR(MAX)) AS re_id,
		CAST(RE_ID_TEXT AS VARCHAR(MAX)) AS re_id_text,
		CAST(RE_PASSIV AS VARCHAR(MAX)) AS re_passiv,
		CAST(RE_TEXT AS VARCHAR(MAX)) AS re_text,
		CONVERT(varchar(max), SA_GILTIG_FOM, 126) AS sa_giltig_fom,
		CONVERT(varchar(max), SA_GILTIG_TOM, 126) AS sa_giltig_tom,
		CAST(SA_ID AS VARCHAR(MAX)) AS sa_id,
		CAST(SA_ID_TEXT AS VARCHAR(MAX)) AS sa_id_text,
		CAST(SA_PASSIV AS VARCHAR(MAX)) AS sa_passiv,
		CAST(SA_TEXT AS VARCHAR(MAX)) AS sa_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_RE ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
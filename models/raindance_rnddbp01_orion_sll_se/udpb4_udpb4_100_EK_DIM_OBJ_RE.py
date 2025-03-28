
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
		CONVERT(varchar(max), RE_GILTIG_FOM, 126) AS RE_GILTIG_FOM,
		CONVERT(varchar(max), RE_GILTIG_TOM, 126) AS RE_GILTIG_TOM,
		CAST(RE_ID AS VARCHAR(MAX)) AS RE_ID,
		CAST(RE_ID_TEXT AS VARCHAR(MAX)) AS RE_ID_TEXT,
		CAST(RE_PASSIV AS VARCHAR(MAX)) AS RE_PASSIV,
		CAST(RE_TEXT AS VARCHAR(MAX)) AS RE_TEXT,
		CONVERT(varchar(max), SA_GILTIG_FOM, 126) AS SA_GILTIG_FOM,
		CONVERT(varchar(max), SA_GILTIG_TOM, 126) AS SA_GILTIG_TOM,
		CAST(SA_ID AS VARCHAR(MAX)) AS SA_ID,
		CAST(SA_ID_TEXT AS VARCHAR(MAX)) AS SA_ID_TEXT,
		CAST(SA_PASSIV AS VARCHAR(MAX)) AS SA_PASSIV,
		CAST(SA_TEXT AS VARCHAR(MAX)) AS SA_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_RE ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
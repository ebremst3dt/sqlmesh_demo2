
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'REGSIGN2': 'varchar(max)', 'REGSIGN2_ID_TEXT': 'varchar(max)', 'REGSIGN_ID_TEXT': 'varchar(max)', 'REGSIGN_TEXT': 'varchar(max)'},
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
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(REGSIGN2 AS VARCHAR(MAX)) AS regsign2,
		CAST(REGSIGN2_ID_TEXT AS VARCHAR(MAX)) AS regsign2_id_text,
		CAST(REGSIGN_ID_TEXT AS VARCHAR(MAX)) AS regsign_id_text,
		CAST(REGSIGN_TEXT AS VARCHAR(MAX)) AS regsign_text 
	FROM udpb4.udpb4_100.EK_DIM_REGSIGN ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
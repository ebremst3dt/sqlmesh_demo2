
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'ATTESTSIGN22': 'varchar(max)', 'ATTESTSIGN22_ID_TEXT': 'varchar(max)', 'ATTESTSIGN2_ID_TEXT': 'varchar(max)', 'ATTESTSIGN2_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(ATTESTSIGN22 AS VARCHAR(MAX)) AS ATTESTSIGN22,
		CAST(ATTESTSIGN22_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN22_ID_TEXT,
		CAST(ATTESTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN2_ID_TEXT,
		CAST(ATTESTSIGN2_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN2_TEXT 
	FROM utdata.utdata295.EK_DIM_ATTESTSIGN2 ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    

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
		'lsfp3_rd_sll_se_utdata_utdata150' as _source,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CAST(REGSIGN2 AS VARCHAR(MAX)) AS REGSIGN2,
		CAST(REGSIGN2_ID_TEXT AS VARCHAR(MAX)) AS REGSIGN2_ID_TEXT,
		CAST(REGSIGN_ID_TEXT AS VARCHAR(MAX)) AS REGSIGN_ID_TEXT,
		CAST(REGSIGN_TEXT AS VARCHAR(MAX)) AS REGSIGN_TEXT 
	FROM utdata.utdata150.EK_DIM_REGSIGN ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
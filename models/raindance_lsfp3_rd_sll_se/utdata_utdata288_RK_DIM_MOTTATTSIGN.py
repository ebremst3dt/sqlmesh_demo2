
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'MOTTATTSIGN2': 'varchar(max)', 'MOTTATTSIGN2_ID_TEXT': 'varchar(max)', 'MOTTATTSIGN_ID_TEXT': 'varchar(max)', 'MOTTATTSIGN_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata288' as _source,
		CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS MOTTATTSIGN,
		CAST(MOTTATTSIGN2 AS VARCHAR(MAX)) AS MOTTATTSIGN2,
		CAST(MOTTATTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN2_ID_TEXT,
		CAST(MOTTATTSIGN_ID_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN_ID_TEXT,
		CAST(MOTTATTSIGN_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN_TEXT 
	FROM utdata.utdata288.RK_DIM_MOTTATTSIGN ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
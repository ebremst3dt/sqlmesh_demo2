
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSTSIGN': 'varchar(max)', 'ANSTSIGN2': 'varchar(max)', 'ANSTSIGN2_ID_TEXT': 'varchar(max)', 'ANSTSIGN_ID_TEXT': 'varchar(max)', 'ANSTSIGN_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CAST(ANSTSIGN AS VARCHAR(MAX)) AS ANSTSIGN,
		CAST(ANSTSIGN2 AS VARCHAR(MAX)) AS ANSTSIGN2,
		CAST(ANSTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS ANSTSIGN2_ID_TEXT,
		CAST(ANSTSIGN_ID_TEXT AS VARCHAR(MAX)) AS ANSTSIGN_ID_TEXT,
		CAST(ANSTSIGN_TEXT AS VARCHAR(MAX)) AS ANSTSIGN_TEXT 
	FROM steudp.udp_600.RK_DIM_ANSTSIGN ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    
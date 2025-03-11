
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANVID_TEXT': 'varchar(max)', 'LOPNUMMER': 'varchar(max)', 'VERDATUM': 'varchar(max)'},
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
    query = """
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ANVID_TEXT AS VARCHAR(MAX)) AS anvid_text,
		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$ANVID) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
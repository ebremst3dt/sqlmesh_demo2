
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'D2': 'varchar(max)', 'D3': 'varchar(max)', 'DD3': 'varchar(max)', 'ID': 'varchar(max)', 'S': 'varchar(max)', 'S1': 'varchar(max)', 'S2': 'varchar(max)', 'S3': 'varchar(max)', 'ZEROS': 'varchar(max)'},
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
		CAST(D2 AS VARCHAR(MAX)) AS d2,
		CAST(D3 AS VARCHAR(MAX)) AS d3,
		CAST(DD3 AS VARCHAR(MAX)) AS dd3,
		CAST(ID AS VARCHAR(MAX)) AS id,
		CAST(S AS VARCHAR(MAX)) AS s,
		CAST(S1 AS VARCHAR(MAX)) AS s1,
		CAST(S2 AS VARCHAR(MAX)) AS s2,
		CAST(S3 AS VARCHAR(MAX)) AS s3,
		CAST(ZEROS AS VARCHAR(MAX)) AS zeros 
	FROM utdata.utdata295.EK_UTILITY) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
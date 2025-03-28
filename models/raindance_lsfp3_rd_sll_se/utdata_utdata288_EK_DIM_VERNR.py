
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AR': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'INTERNVERNR_TEXT': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERNRGRUPP': 'varchar(max)'},
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
		CAST(AR AS VARCHAR(MAX)) AS AR,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(INTERNVERNR_TEXT AS VARCHAR(MAX)) AS INTERNVERNR_TEXT,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERNRGRUPP AS VARCHAR(MAX)) AS VERNRGRUPP 
	FROM utdata.utdata288.EK_DIM_VERNR ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
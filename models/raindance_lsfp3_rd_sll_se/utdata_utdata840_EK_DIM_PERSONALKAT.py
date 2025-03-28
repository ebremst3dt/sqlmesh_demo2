
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PERSONALKAT_ID': 'varchar(max)', 'PERSONALKAT_ID_TEXT': 'varchar(max)', 'PERSONALKAT_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata840' as _source,
		CAST(PERSONALKAT_ID AS VARCHAR(MAX)) AS PERSONALKAT_ID,
		CAST(PERSONALKAT_ID_TEXT AS VARCHAR(MAX)) AS PERSONALKAT_ID_TEXT,
		CAST(PERSONALKAT_TEXT AS VARCHAR(MAX)) AS PERSONALKAT_TEXT 
	FROM utdata.utdata840.EK_DIM_PERSONALKAT ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
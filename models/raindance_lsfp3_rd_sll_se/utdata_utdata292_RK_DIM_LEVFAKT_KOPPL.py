
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KORR1': 'varchar(max)', 'KORR2': 'varchar(max)', 'KORR3': 'varchar(max)', 'KORR4': 'varchar(max)', 'KORR5': 'varchar(max)', 'KORR6': 'varchar(max)', 'KORR7': 'varchar(max)', 'KORR8': 'varchar(max)', 'KORR9': 'varchar(max)', 'KORR10': 'varchar(max)', 'NR': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata292' as _source,
		CAST(KORR1 AS VARCHAR(MAX)) AS KORR1,
		CAST(KORR2 AS VARCHAR(MAX)) AS KORR2,
		CAST(KORR3 AS VARCHAR(MAX)) AS KORR3,
		CAST(KORR4 AS VARCHAR(MAX)) AS KORR4,
		CAST(KORR5 AS VARCHAR(MAX)) AS KORR5,
		CAST(KORR6 AS VARCHAR(MAX)) AS KORR6,
		CAST(KORR7 AS VARCHAR(MAX)) AS KORR7,
		CAST(KORR8 AS VARCHAR(MAX)) AS KORR8,
		CAST(KORR9 AS VARCHAR(MAX)) AS KORR9,
		CAST(KORR10 AS VARCHAR(MAX)) AS KORR10,
		CAST(NR AS VARCHAR(MAX)) AS NR 
	FROM utdata.utdata292.RK_DIM_LEVFAKT_KOPPL ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
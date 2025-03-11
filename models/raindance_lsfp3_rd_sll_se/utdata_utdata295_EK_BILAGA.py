
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'BILAGA1': 'varchar(max)', 'BILAGA2': 'varchar(max)', 'BILAGA3': 'varchar(max)', 'BILAGA4': 'varchar(max)', 'BILAGA5': 'varchar(max)', 'BILAGA6': 'varchar(max)', 'BILAGA7': 'varchar(max)', 'BILAGA8': 'varchar(max)', 'BILAGA9': 'varchar(max)', 'BILAGA10': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)'},
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
		CAST(BILAGA1 AS VARCHAR(MAX)) AS bilaga1,
		CAST(BILAGA2 AS VARCHAR(MAX)) AS bilaga2,
		CAST(BILAGA3 AS VARCHAR(MAX)) AS bilaga3,
		CAST(BILAGA4 AS VARCHAR(MAX)) AS bilaga4,
		CAST(BILAGA5 AS VARCHAR(MAX)) AS bilaga5,
		CAST(BILAGA6 AS VARCHAR(MAX)) AS bilaga6,
		CAST(BILAGA7 AS VARCHAR(MAX)) AS bilaga7,
		CAST(BILAGA8 AS VARCHAR(MAX)) AS bilaga8,
		CAST(BILAGA9 AS VARCHAR(MAX)) AS bilaga9,
		CAST(BILAGA10 AS VARCHAR(MAX)) AS bilaga10,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid 
	FROM utdata.utdata295.EK_BILAGA) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
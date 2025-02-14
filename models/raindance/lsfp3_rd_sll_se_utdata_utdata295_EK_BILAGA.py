
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'BILAGA1': 'varchar(max)',
 'BILAGA10': 'varchar(max)',
 'BILAGA2': 'varchar(max)',
 'BILAGA3': 'varchar(max)',
 'BILAGA4': 'varchar(max)',
 'BILAGA5': 'varchar(max)',
 'BILAGA6': 'varchar(max)',
 'BILAGA7': 'varchar(max)',
 'BILAGA8': 'varchar(max)',
 'BILAGA9': 'varchar(max)',
 'DOKTYP': 'varchar(max)',
 'DOKUMENTID': 'varchar(max)'},
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
	SELECT top 1000
 		CAST(BILAGA1 AS VARCHAR(MAX)) AS bilaga1,
		CAST(BILAGA10 AS VARCHAR(MAX)) AS bilaga10,
		CAST(BILAGA2 AS VARCHAR(MAX)) AS bilaga2,
		CAST(BILAGA3 AS VARCHAR(MAX)) AS bilaga3,
		CAST(BILAGA4 AS VARCHAR(MAX)) AS bilaga4,
		CAST(BILAGA5 AS VARCHAR(MAX)) AS bilaga5,
		CAST(BILAGA6 AS VARCHAR(MAX)) AS bilaga6,
		CAST(BILAGA7 AS VARCHAR(MAX)) AS bilaga7,
		CAST(BILAGA8 AS VARCHAR(MAX)) AS bilaga8,
		CAST(BILAGA9 AS VARCHAR(MAX)) AS bilaga9,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid 
	FROM utdata.utdata295.EK_BILAGA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

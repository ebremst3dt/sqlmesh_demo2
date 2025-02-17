
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BILAGA1': 'varchar(200)',
 'BILAGA10': 'varchar(200)',
 'BILAGA2': 'varchar(200)',
 'BILAGA3': 'varchar(200)',
 'BILAGA4': 'varchar(200)',
 'BILAGA5': 'varchar(200)',
 'BILAGA6': 'varchar(200)',
 'BILAGA7': 'varchar(200)',
 'BILAGA8': 'varchar(200)',
 'BILAGA9': 'varchar(200)',
 'DOKTYP': 'numeric',
 'DOKUMENTID': 'varchar(20)'},
    kind=ModelKindName.FULL,
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

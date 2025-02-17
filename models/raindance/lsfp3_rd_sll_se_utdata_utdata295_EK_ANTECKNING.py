
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANTECKNING': 'varchar(4000)',
 'ANTECKNING1': 'varchar(4000)',
 'ANTECKNING10': 'varchar(4000)',
 'ANTECKNING2': 'varchar(4000)',
 'ANTECKNING3': 'varchar(4000)',
 'ANTECKNING4': 'varchar(4000)',
 'ANTECKNING5': 'varchar(4000)',
 'ANTECKNING6': 'varchar(4000)',
 'ANTECKNING7': 'varchar(4000)',
 'ANTECKNING8': 'varchar(4000)',
 'ANTECKNING9': 'varchar(4000)',
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
 		CAST(ANTECKNING AS VARCHAR(MAX)) AS anteckning,
		CAST(ANTECKNING1 AS VARCHAR(MAX)) AS anteckning1,
		CAST(ANTECKNING10 AS VARCHAR(MAX)) AS anteckning10,
		CAST(ANTECKNING2 AS VARCHAR(MAX)) AS anteckning2,
		CAST(ANTECKNING3 AS VARCHAR(MAX)) AS anteckning3,
		CAST(ANTECKNING4 AS VARCHAR(MAX)) AS anteckning4,
		CAST(ANTECKNING5 AS VARCHAR(MAX)) AS anteckning5,
		CAST(ANTECKNING6 AS VARCHAR(MAX)) AS anteckning6,
		CAST(ANTECKNING7 AS VARCHAR(MAX)) AS anteckning7,
		CAST(ANTECKNING8 AS VARCHAR(MAX)) AS anteckning8,
		CAST(ANTECKNING9 AS VARCHAR(MAX)) AS anteckning9,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid 
	FROM utdata.utdata295.EK_ANTECKNING
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

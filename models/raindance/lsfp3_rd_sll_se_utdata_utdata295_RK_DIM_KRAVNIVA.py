
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'KRAVNIVA': 'varchar(max)', 'KRAVNIVA_TEXT': 'varchar(max)'},
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
 		CAST(KRAVNIVA AS VARCHAR(MAX)) AS kravniva,
		CAST(KRAVNIVA_TEXT AS VARCHAR(MAX)) AS kravniva_text 
	FROM utdata.utdata295.RK_DIM_KRAVNIVA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ATTRIBUTE': 'varchar(120)', 'ATTR_KEY_PAT': 'numeric', 'SBID': 'varchar(16)'},
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
 		CAST(ATTR_KEY_PAT AS VARCHAR(MAX)) AS attr_key_pat,
		CAST(ATTRIBUTE AS VARCHAR(MAX)) AS attribute,
		CAST(SBID AS VARCHAR(MAX)) AS sbid 
	FROM utdata.utdata295.RK_DIM_LEV_ATTR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

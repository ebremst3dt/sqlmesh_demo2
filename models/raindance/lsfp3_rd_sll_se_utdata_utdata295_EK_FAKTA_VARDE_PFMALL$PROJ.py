
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'MALLID_ID': 'varchar(20)', 'PROJ_ID': 'varchar(120)', 'RAK_ID': 'varchar(20)'},
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
 		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$PROJ
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'FRI1_GILTIG_FOM': 'datetime',
 'FRI1_GILTIG_TOM': 'datetime',
 'FRI1_ID': 'varchar(4)',
 'FRI1_ID_TEXT': 'varchar(35)',
 'FRI1_PASSIV': 'bit',
 'FRI1_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), FRI1_GILTIG_FOM, 126) AS fri1_giltig_fom,
		CONVERT(varchar(max), FRI1_GILTIG_TOM, 126) AS fri1_giltig_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI1_ID_TEXT AS VARCHAR(MAX)) AS fri1_id_text,
		CAST(FRI1_PASSIV AS VARCHAR(MAX)) AS fri1_passiv,
		CAST(FRI1_TEXT AS VARCHAR(MAX)) AS fri1_text 
	FROM utdata.utdata295.EK_DIM_OBJ_FRI1
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

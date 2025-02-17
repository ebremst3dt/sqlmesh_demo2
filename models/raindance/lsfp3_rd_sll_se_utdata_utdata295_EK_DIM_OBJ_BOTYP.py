
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOTYP_GILTIG_FOM': 'datetime',
 'BOTYP_GILTIG_TOM': 'datetime',
 'BOTYP_ID': 'varchar(1)',
 'BOTYP_ID_TEXT': 'varchar(32)',
 'BOTYP_PASSIV': 'bit',
 'BOTYP_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), BOTYP_GILTIG_FOM, 126) AS botyp_giltig_fom,
		CONVERT(varchar(max), BOTYP_GILTIG_TOM, 126) AS botyp_giltig_tom,
		CAST(BOTYP_ID AS VARCHAR(MAX)) AS botyp_id,
		CAST(BOTYP_ID_TEXT AS VARCHAR(MAX)) AS botyp_id_text,
		CAST(BOTYP_PASSIV AS VARCHAR(MAX)) AS botyp_passiv,
		CAST(BOTYP_TEXT AS VARCHAR(MAX)) AS botyp_text 
	FROM utdata.utdata295.EK_DIM_OBJ_BOTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

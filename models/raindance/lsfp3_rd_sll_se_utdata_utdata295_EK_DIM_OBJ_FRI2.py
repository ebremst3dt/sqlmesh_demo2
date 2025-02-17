
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'FRI2_GILTIG_FOM': 'datetime',
 'FRI2_GILTIG_TOM': 'datetime',
 'FRI2_ID': 'varchar(3)',
 'FRI2_ID_TEXT': 'varchar(34)',
 'FRI2_PASSIV': 'bit',
 'FRI2_TEXT': 'varchar(30)'},
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
 		CAST(FRI2_GILTIG_FOM AS VARCHAR(MAX)) AS fri2_giltig_fom,
		CAST(FRI2_GILTIG_TOM AS VARCHAR(MAX)) AS fri2_giltig_tom,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(FRI2_ID_TEXT AS VARCHAR(MAX)) AS fri2_id_text,
		CAST(FRI2_PASSIV AS VARCHAR(MAX)) AS fri2_passiv,
		CAST(FRI2_TEXT AS VARCHAR(MAX)) AS fri2_text 
	FROM utdata.utdata295.EK_DIM_OBJ_FRI2
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

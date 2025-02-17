
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANST_GILTIG_FOM': 'datetime',
 'ANST_GILTIG_TOM': 'datetime',
 'ANST_ID': 'varchar(20)',
 'ANST_ID_TEXT': 'varchar(51)',
 'ANST_PASSIV': 'bit',
 'ANST_TEXT': 'varchar(30)'},
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
 		CAST(ANST_GILTIG_FOM AS VARCHAR(MAX)) AS anst_giltig_fom,
		CAST(ANST_GILTIG_TOM AS VARCHAR(MAX)) AS anst_giltig_tom,
		CAST(ANST_ID AS VARCHAR(MAX)) AS anst_id,
		CAST(ANST_ID_TEXT AS VARCHAR(MAX)) AS anst_id_text,
		CAST(ANST_PASSIV AS VARCHAR(MAX)) AS anst_passiv,
		CAST(ANST_TEXT AS VARCHAR(MAX)) AS anst_text 
	FROM utdata.utdata295.EK_DIM_OBJ_ANST
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

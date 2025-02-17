
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'URS_GILTIG_FOM': 'datetime',
 'URS_GILTIG_TOM': 'datetime',
 'URS_ID': 'varchar(2)',
 'URS_ID_TEXT': 'varchar(33)',
 'URS_PASSIV': 'bit',
 'URS_TEXT': 'varchar(30)'},
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
 		CAST(URS_GILTIG_FOM AS VARCHAR(MAX)) AS urs_giltig_fom,
		CAST(URS_GILTIG_TOM AS VARCHAR(MAX)) AS urs_giltig_tom,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(URS_ID_TEXT AS VARCHAR(MAX)) AS urs_id_text,
		CAST(URS_PASSIV AS VARCHAR(MAX)) AS urs_passiv,
		CAST(URS_TEXT AS VARCHAR(MAX)) AS urs_text 
	FROM utdata.utdata295.EK_DIM_OBJ_URS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

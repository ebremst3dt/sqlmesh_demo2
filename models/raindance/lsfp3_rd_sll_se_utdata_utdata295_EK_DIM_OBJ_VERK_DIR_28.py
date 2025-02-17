
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'VERK_GILTIG_FOM': 'datetime',
 'VERK_GILTIG_TOM': 'datetime',
 'VERK_ID': 'varchar(2)',
 'VERK_ID_TEXT': 'varchar(33)',
 'VERK_PASSIV': 'bit',
 'VERK_TEXT': 'varchar(30)',
 'VGREN_GILTIG_FOM': 'datetime',
 'VGREN_GILTIG_TOM': 'datetime',
 'VGREN_ID': 'varchar(4)',
 'VGREN_ID_TEXT': 'varchar(35)',
 'VGREN_PASSIV': 'bit',
 'VGREN_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS verk_giltig_fom,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS verk_giltig_tom,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS verk_id_text,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS verk_passiv,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS verk_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text 
	FROM utdata.utdata295.EK_DIM_OBJ_VERK_DIR_28
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

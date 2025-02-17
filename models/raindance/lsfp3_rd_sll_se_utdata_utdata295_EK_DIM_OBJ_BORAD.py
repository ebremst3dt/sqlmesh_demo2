
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BORAD_GILTIG_FOM': 'varchar(max)',
 'BORAD_GILTIG_TOM': 'varchar(max)',
 'BORAD_ID': 'varchar(max)',
 'BORAD_ID_TEXT': 'varchar(max)',
 'BORAD_PASSIV': 'varchar(max)',
 'BORAD_TEXT': 'varchar(max)'},
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
 		CAST(BORAD_GILTIG_FOM AS VARCHAR(MAX)) AS borad_giltig_fom,
		CAST(BORAD_GILTIG_TOM AS VARCHAR(MAX)) AS borad_giltig_tom,
		CAST(BORAD_ID AS VARCHAR(MAX)) AS borad_id,
		CAST(BORAD_ID_TEXT AS VARCHAR(MAX)) AS borad_id_text,
		CAST(BORAD_PASSIV AS VARCHAR(MAX)) AS borad_passiv,
		CAST(BORAD_TEXT AS VARCHAR(MAX)) AS borad_text 
	FROM utdata.utdata295.EK_DIM_OBJ_BORAD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANSTALLD_GILTIG_FOM': 'varchar(max)',
 'ANSTALLD_GILTIG_TOM': 'varchar(max)',
 'ANSTALLD_ID': 'varchar(max)',
 'ANSTALLD_ID_TEXT': 'varchar(max)',
 'ANSTALLD_PASSIV': 'varchar(max)',
 'ANSTALLD_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), ANSTALLD_GILTIG_FOM, 126) AS anstalld_giltig_fom,
		CONVERT(varchar(max), ANSTALLD_GILTIG_TOM, 126) AS anstalld_giltig_tom,
		CAST(ANSTALLD_ID AS VARCHAR(MAX)) AS anstalld_id,
		CAST(ANSTALLD_ID_TEXT AS VARCHAR(MAX)) AS anstalld_id_text,
		CAST(ANSTALLD_PASSIV AS VARCHAR(MAX)) AS anstalld_passiv,
		CAST(ANSTALLD_TEXT AS VARCHAR(MAX)) AS anstalld_text 
	FROM utdata.utdata295.EK_DIM_ANSTALLD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

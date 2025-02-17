
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'MOMS_GILTIG_FOM': 'varchar(max)',
 'MOMS_GILTIG_TOM': 'varchar(max)',
 'MOMS_ID': 'varchar(max)',
 'MOMS_ID_TEXT': 'varchar(max)',
 'MOMS_PASSIV': 'varchar(max)',
 'MOMS_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
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
 		CAST(MOMS_GILTIG_FOM AS VARCHAR(MAX)) AS moms_giltig_fom,
		CAST(MOMS_GILTIG_TOM AS VARCHAR(MAX)) AS moms_giltig_tom,
		CAST(MOMS_ID AS VARCHAR(MAX)) AS moms_id,
		CAST(MOMS_ID_TEXT AS VARCHAR(MAX)) AS moms_id_text,
		CAST(MOMS_PASSIV AS VARCHAR(MAX)) AS moms_passiv,
		CAST(MOMS_TEXT AS VARCHAR(MAX)) AS moms_text 
	FROM utdata.utdata295.EK_DIM_OBJ_MOMS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

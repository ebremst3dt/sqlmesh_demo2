
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'DETALJTYP': 'varchar(max)',
 'DETALJTYP_ID': 'varchar(max)',
 'DETALJTYP_ID_TEXT': 'varchar(max)',
 'DETALJTYP_NR': 'varchar(max)',
 'DETALJTYP_TEXT': 'varchar(max)'},
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
 		CAST(DETALJTYP AS VARCHAR(MAX)) AS detaljtyp,
		CAST(DETALJTYP_ID AS VARCHAR(MAX)) AS detaljtyp_id,
		CAST(DETALJTYP_ID_TEXT AS VARCHAR(MAX)) AS detaljtyp_id_text,
		CAST(DETALJTYP_NR AS VARCHAR(MAX)) AS detaljtyp_nr,
		CAST(DETALJTYP_TEXT AS VARCHAR(MAX)) AS detaljtyp_text 
	FROM utdata.utdata295.RK_DIM_DETALJTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

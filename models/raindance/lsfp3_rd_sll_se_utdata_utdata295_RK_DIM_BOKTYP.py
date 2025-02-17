
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'BOKTYP': 'varchar(max)',
 'BOKTYP_ID': 'varchar(max)',
 'BOKTYP_ID_TEXT': 'varchar(max)',
 'BOKTYP_NR': 'varchar(max)',
 'BOKTYP_TEXT': 'varchar(max)'},
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
 		CAST(BOKTYP AS VARCHAR(MAX)) AS boktyp,
		CAST(BOKTYP_ID AS VARCHAR(MAX)) AS boktyp_id,
		CAST(BOKTYP_ID_TEXT AS VARCHAR(MAX)) AS boktyp_id_text,
		CAST(BOKTYP_NR AS VARCHAR(MAX)) AS boktyp_nr,
		CAST(BOKTYP_TEXT AS VARCHAR(MAX)) AS boktyp_text 
	FROM utdata.utdata295.RK_DIM_BOKTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

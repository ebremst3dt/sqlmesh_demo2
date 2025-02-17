
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOKTYP': 'varchar(2)',
 'BOKTYP_ID': 'varchar(2)',
 'BOKTYP_ID_TEXT': 'varchar(41)',
 'BOKTYP_NR': 'varchar(2)',
 'BOKTYP_TEXT': 'varchar(38)'},
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
 		CAST(BOKTYP AS VARCHAR(MAX)) AS boktyp,
		CAST(BOKTYP_ID AS VARCHAR(MAX)) AS boktyp_id,
		CAST(BOKTYP_ID_TEXT AS VARCHAR(MAX)) AS boktyp_id_text,
		CAST(BOKTYP_NR AS VARCHAR(MAX)) AS boktyp_nr,
		CAST(BOKTYP_TEXT AS VARCHAR(MAX)) AS boktyp_text 
	FROM utdata.utdata295.RK_DIM_BOKTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'DELSYS': 'varchar(max)',
 'DELSYS_TEXT': 'varchar(max)',
 'DOKUMENTTYP': 'varchar(max)',
 'EXTERNID': 'varchar(max)',
 'EXTERNID2': 'varchar(max)',
 'EXTERNID2_ID_TEXT': 'varchar(max)',
 'EXTERNID_GRUPP': 'varchar(max)',
 'EXTERNID_ID_TEXT': 'varchar(max)',
 'EXTERNID_TEXT': 'varchar(max)',
 'NAMN2': 'varchar(max)'},
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
 		CAST(DELSYS AS VARCHAR(MAX)) AS delsys,
		CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS delsys_text,
		CAST(DOKUMENTTYP AS VARCHAR(MAX)) AS dokumenttyp,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNID_GRUPP AS VARCHAR(MAX)) AS externid_grupp,
		CAST(EXTERNID_ID_TEXT AS VARCHAR(MAX)) AS externid_id_text,
		CAST(EXTERNID_TEXT AS VARCHAR(MAX)) AS externid_text,
		CAST(EXTERNID2 AS VARCHAR(MAX)) AS externid2,
		CAST(EXTERNID2_ID_TEXT AS VARCHAR(MAX)) AS externid2_id_text,
		CAST(NAMN2 AS VARCHAR(MAX)) AS namn2 
	FROM utdata.utdata295.EK_DIM_EXTERNID
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

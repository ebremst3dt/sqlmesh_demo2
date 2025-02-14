
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'BOKBELOPP_INT': 'varchar(max)',
 'BOKBELOPP_VAL': 'varchar(max)',
 'BOKSTATUS': 'varchar(max)',
 'BOKTYP': 'varchar(max)',
 'DETALJTYP': 'varchar(max)',
 'NR': 'varchar(max)',
 'TAB_MOMS': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VERNR': 'varchar(max)',
 'VERRAD': 'varchar(max)'},
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
 		CAST(BOKBELOPP_INT AS VARCHAR(MAX)) AS bokbelopp_int,
		CAST(BOKBELOPP_VAL AS VARCHAR(MAX)) AS bokbelopp_val,
		CAST(BOKSTATUS AS VARCHAR(MAX)) AS bokstatus,
		CAST(BOKTYP AS VARCHAR(MAX)) AS boktyp,
		CAST(DETALJTYP AS VARCHAR(MAX)) AS detaljtyp,
		CAST(NR AS VARCHAR(MAX)) AS nr,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad 
	FROM utdata.utdata295.RK_FAKTA_KUNDBOKDET
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

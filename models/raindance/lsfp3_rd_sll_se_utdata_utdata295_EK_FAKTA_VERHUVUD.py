
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANTALFEL': 'varchar(max)',
 'ANTALRADER': 'varchar(max)',
 'BOKFORINGSAR': 'varchar(max)',
 'DEFDATUM': 'varchar(max)',
 'DEFSIGN': 'varchar(max)',
 'FTG': 'varchar(max)',
 'HUVUDTEXT': 'varchar(max)',
 'INTERNVERNR': 'varchar(max)',
 'REGDATUM': 'varchar(max)',
 'REGSIGN': 'varchar(max)',
 'STATUS': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VERNR': 'varchar(max)',
 'VERTYP': 'varchar(max)'},
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
 		CAST(ANTALFEL AS VARCHAR(MAX)) AS antalfel,
		CAST(ANTALRADER AS VARCHAR(MAX)) AS antalrader,
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS bokforingsar,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(FTG AS VARCHAR(MAX)) AS ftg,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp 
	FROM utdata.utdata295.EK_FAKTA_VERHUVUD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


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
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANTALFEL AS VARCHAR(MAX)) AS ANTALFEL,
CAST(ANTALRADER AS VARCHAR(MAX)) AS ANTALRADER,
CAST(BOKFORINGSAR AS VARCHAR(MAX)) AS BOKFORINGSAR,
CAST(DEFDATUM AS VARCHAR(MAX)) AS DEFDATUM,
CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
CAST(FTG AS VARCHAR(MAX)) AS FTG,
CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
CAST(REGDATUM AS VARCHAR(MAX)) AS REGDATUM,
CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP FROM utdata.utdata295.EK_FAKTA_VERHUVUD"""
    return pipe(query=query)

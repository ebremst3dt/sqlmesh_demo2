
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


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
    query = """SELECT CAST(BOKBELOPP_INT AS VARCHAR(MAX)) AS BOKBELOPP_INT,
CAST(BOKBELOPP_VAL AS VARCHAR(MAX)) AS BOKBELOPP_VAL,
CAST(BOKSTATUS AS VARCHAR(MAX)) AS BOKSTATUS,
CAST(BOKTYP AS VARCHAR(MAX)) AS BOKTYP,
CAST(DETALJTYP AS VARCHAR(MAX)) AS DETALJTYP,
CAST(NR AS VARCHAR(MAX)) AS NR,
CAST(TAB_MOMS AS VARCHAR(MAX)) AS TAB_MOMS,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD FROM utdata.utdata295.RK_FAKTA_KUNDBOKDET"""
    return pipe(query=query)

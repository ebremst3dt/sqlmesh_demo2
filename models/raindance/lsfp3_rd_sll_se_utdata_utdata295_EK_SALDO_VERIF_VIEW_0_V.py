
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AVTBES_ID': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'UTFALL_V': 'varchar(max)',
 'VERK_ID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID FROM utdata.utdata295.EK_SALDO_VERIF_VIEW_0_V"""
    return pipe(query=query)

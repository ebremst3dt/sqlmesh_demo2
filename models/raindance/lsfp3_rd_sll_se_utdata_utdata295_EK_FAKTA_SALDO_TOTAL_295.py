
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AVTBES_ID': 'varchar(max)',
 'BFAR': 'varchar(max)',
 'BUDGET_ACK': 'varchar(max)',
 'BUDGET_AR': 'varchar(max)',
 'BUDGET_ARSACK': 'varchar(max)',
 'BUDGET_FGACK': 'varchar(max)',
 'BUDGET_FGAR': 'varchar(max)',
 'BUDGET_FGPER': 'varchar(max)',
 'BUDGET_PER': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'LFRAM_AR': 'varchar(max)',
 'LFRAM_PER': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'PERIOD': 'varchar(max)',
 'PRG_FGPER': 'varchar(max)',
 'PRG_PER': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'UTFALL_ACK': 'varchar(max)',
 'UTFALL_AR': 'varchar(max)',
 'UTFALL_ARSACK': 'varchar(max)',
 'UTFALL_FGACK': 'varchar(max)',
 'UTFALL_FGAR': 'varchar(max)',
 'UTFALL_FGPER': 'varchar(max)',
 'UTFALL_IB': 'varchar(max)',
 'UTFALL_PER': 'varchar(max)',
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
CAST(BFAR AS VARCHAR(MAX)) AS BFAR,
CAST(BUDGET_ACK AS VARCHAR(MAX)) AS BUDGET_ACK,
CAST(BUDGET_AR AS VARCHAR(MAX)) AS BUDGET_AR,
CAST(BUDGET_ARSACK AS VARCHAR(MAX)) AS BUDGET_ARSACK,
CAST(BUDGET_FGACK AS VARCHAR(MAX)) AS BUDGET_FGACK,
CAST(BUDGET_FGAR AS VARCHAR(MAX)) AS BUDGET_FGAR,
CAST(BUDGET_FGPER AS VARCHAR(MAX)) AS BUDGET_FGPER,
CAST(BUDGET_PER AS VARCHAR(MAX)) AS BUDGET_PER,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(LFRAM_AR AS VARCHAR(MAX)) AS LFRAM_AR,
CAST(LFRAM_PER AS VARCHAR(MAX)) AS LFRAM_PER,
CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
CAST(PERIOD AS VARCHAR(MAX)) AS PERIOD,
CAST(PRG_FGPER AS VARCHAR(MAX)) AS PRG_FGPER,
CAST(PRG_PER AS VARCHAR(MAX)) AS PRG_PER,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(UTFALL_ACK AS VARCHAR(MAX)) AS UTFALL_ACK,
CAST(UTFALL_AR AS VARCHAR(MAX)) AS UTFALL_AR,
CAST(UTFALL_ARSACK AS VARCHAR(MAX)) AS UTFALL_ARSACK,
CAST(UTFALL_FGACK AS VARCHAR(MAX)) AS UTFALL_FGACK,
CAST(UTFALL_FGAR AS VARCHAR(MAX)) AS UTFALL_FGAR,
CAST(UTFALL_FGPER AS VARCHAR(MAX)) AS UTFALL_FGPER,
CAST(UTFALL_IB AS VARCHAR(MAX)) AS UTFALL_IB,
CAST(UTFALL_PER AS VARCHAR(MAX)) AS UTFALL_PER,
CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID FROM utdata.utdata295.EK_FAKTA_SALDO_TOTAL_295"""
    return pipe(query=query)

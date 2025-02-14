
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'INTKUNDID': 'varchar(max)',
 'INTLEVID': 'varchar(max)',
 'KUNDFAKTNR': 'varchar(max)',
 'KUND_PÅLOGG_FTG': 'varchar(max)',
 'LEV_PÅLOGG_FTG': 'varchar(max)',
 'MOTPKOMB': 'varchar(max)',
 'NR': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(INTKUNDID AS VARCHAR(MAX)) AS INTKUNDID,
CAST(INTLEVID AS VARCHAR(MAX)) AS INTLEVID,
CAST(KUND_PÅLOGG_FTG AS VARCHAR(MAX)) AS KUND_PÅLOGG_FTG,
CAST(KUNDFAKTNR AS VARCHAR(MAX)) AS KUNDFAKTNR,
CAST(LEV_PÅLOGG_FTG AS VARCHAR(MAX)) AS LEV_PÅLOGG_FTG,
CAST(MOTPKOMB AS VARCHAR(MAX)) AS MOTPKOMB,
CAST(NR AS VARCHAR(MAX)) AS NR FROM utdata.utdata295.RK_DIM_INTLEVFAKT_MOTP"""
    return pipe(query=query)

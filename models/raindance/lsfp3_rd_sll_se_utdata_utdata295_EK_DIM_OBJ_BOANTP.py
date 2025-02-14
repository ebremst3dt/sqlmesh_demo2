
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BOANTP_GILTIG_FOM': 'varchar(max)',
 'BOANTP_GILTIG_TOM': 'varchar(max)',
 'BOANTP_ID': 'varchar(max)',
 'BOANTP_ID_TEXT': 'varchar(max)',
 'BOANTP_PASSIV': 'varchar(max)',
 'BOANTP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BOANTP_GILTIG_FOM AS VARCHAR(MAX)) AS BOANTP_GILTIG_FOM,
CAST(BOANTP_GILTIG_TOM AS VARCHAR(MAX)) AS BOANTP_GILTIG_TOM,
CAST(BOANTP_ID AS VARCHAR(MAX)) AS BOANTP_ID,
CAST(BOANTP_ID_TEXT AS VARCHAR(MAX)) AS BOANTP_ID_TEXT,
CAST(BOANTP_PASSIV AS VARCHAR(MAX)) AS BOANTP_PASSIV,
CAST(BOANTP_TEXT AS VARCHAR(MAX)) AS BOANTP_TEXT FROM utdata.utdata295.EK_DIM_OBJ_BOANTP"""
    return pipe(query=query)


import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BIT_PAF': 'varchar(max)',
 'ENVELOPE_TRS': 'varchar(max)',
 'FORMATV_RDF': 'varchar(max)',
 'FREEVALUE': 'varchar(max)',
 'LOGICALVALUE': 'varchar(max)',
 'MEDIA_TRS': 'varchar(max)',
 'MSGKEY': 'varchar(max)',
 'MSGTYPEV': 'varchar(max)',
 'MSGWAY': 'varchar(max)',
 'PART': 'varchar(max)',
 'SBID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BIT_PAF AS VARCHAR(MAX)) AS BIT_PAF,
CAST(ENVELOPE_TRS AS VARCHAR(MAX)) AS ENVELOPE_TRS,
CAST(FORMATV_RDF AS VARCHAR(MAX)) AS FORMATV_RDF,
CAST(FREEVALUE AS VARCHAR(MAX)) AS FREEVALUE,
CAST(LOGICALVALUE AS VARCHAR(MAX)) AS LOGICALVALUE,
CAST(MEDIA_TRS AS VARCHAR(MAX)) AS MEDIA_TRS,
CAST(MSGKEY AS VARCHAR(MAX)) AS MSGKEY,
CAST(MSGTYPEV AS VARCHAR(MAX)) AS MSGTYPEV,
CAST(MSGWAY AS VARCHAR(MAX)) AS MSGWAY,
CAST(PART AS VARCHAR(MAX)) AS PART,
CAST(SBID AS VARCHAR(MAX)) AS SBID FROM utdata.utdata295.RK_DIM_LEV_MSG"""
    return pipe(query=query)

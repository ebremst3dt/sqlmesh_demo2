
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'MOTFRA_GILTIG_FOM': 'varchar(max)',
 'MOTFRA_GILTIG_TOM': 'varchar(max)',
 'MOTFRA_ID': 'varchar(max)',
 'MOTFRA_ID_TEXT': 'varchar(max)',
 'MOTFRA_PASSIV': 'varchar(max)',
 'MOTFRA_TEXT': 'varchar(max)',
 'MOTP_DATUM_FOM': 'varchar(max)',
 'MOTP_DATUM_TOM': 'varchar(max)',
 'MOTP_GILTIG_FOM': 'varchar(max)',
 'MOTP_GILTIG_TOM': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'MOTP_ID_TEXT': 'varchar(max)',
 'MOTP_PASSIV': 'varchar(max)',
 'MOTP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(MOTFRA_GILTIG_FOM AS VARCHAR(MAX)) AS MOTFRA_GILTIG_FOM,
CAST(MOTFRA_GILTIG_TOM AS VARCHAR(MAX)) AS MOTFRA_GILTIG_TOM,
CAST(MOTFRA_ID AS VARCHAR(MAX)) AS MOTFRA_ID,
CAST(MOTFRA_ID_TEXT AS VARCHAR(MAX)) AS MOTFRA_ID_TEXT,
CAST(MOTFRA_PASSIV AS VARCHAR(MAX)) AS MOTFRA_PASSIV,
CAST(MOTFRA_TEXT AS VARCHAR(MAX)) AS MOTFRA_TEXT,
CAST(MOTP_DATUM_FOM AS VARCHAR(MAX)) AS MOTP_DATUM_FOM,
CAST(MOTP_DATUM_TOM AS VARCHAR(MAX)) AS MOTP_DATUM_TOM,
CAST(MOTP_GILTIG_FOM AS VARCHAR(MAX)) AS MOTP_GILTIG_FOM,
CAST(MOTP_GILTIG_TOM AS VARCHAR(MAX)) AS MOTP_GILTIG_TOM,
CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_MOTP"""
    return pipe(query=query)

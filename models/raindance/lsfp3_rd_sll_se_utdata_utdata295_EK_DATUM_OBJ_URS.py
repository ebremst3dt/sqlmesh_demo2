
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'URS_DATUM_FOM': 'varchar(max)',
 'URS_DATUM_TOM': 'varchar(max)',
 'URS_GILTIG_FOM': 'varchar(max)',
 'URS_GILTIG_TOM': 'varchar(max)',
 'URS_ID': 'varchar(max)',
 'URS_ID_TEXT': 'varchar(max)',
 'URS_PASSIV': 'varchar(max)',
 'URS_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(URS_DATUM_FOM AS VARCHAR(MAX)) AS URS_DATUM_FOM,
CAST(URS_DATUM_TOM AS VARCHAR(MAX)) AS URS_DATUM_TOM,
CAST(URS_GILTIG_FOM AS VARCHAR(MAX)) AS URS_GILTIG_FOM,
CAST(URS_GILTIG_TOM AS VARCHAR(MAX)) AS URS_GILTIG_TOM,
CAST(URS_ID AS VARCHAR(MAX)) AS URS_ID,
CAST(URS_ID_TEXT AS VARCHAR(MAX)) AS URS_ID_TEXT,
CAST(URS_PASSIV AS VARCHAR(MAX)) AS URS_PASSIV,
CAST(URS_TEXT AS VARCHAR(MAX)) AS URS_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_URS"""
    return pipe(query=query)

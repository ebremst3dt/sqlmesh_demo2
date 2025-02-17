
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ARTKR_ID': 'varchar(20)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'MOMS_ID': 'varchar(120)'},
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
 		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(MOMS_ID AS VARCHAR(MAX)) AS moms_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARTKR$MOMS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

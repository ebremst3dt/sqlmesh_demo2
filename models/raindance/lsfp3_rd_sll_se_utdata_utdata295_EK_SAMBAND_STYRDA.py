
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'NUMERISK_VANSTER': 'varchar(1)',
 'STYRD_ID': 'varchar(6)',
 'STYRD_NR': 'numeric',
 'URVAL': 'varchar(1)'},
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
 		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(NUMERISK_VANSTER AS VARCHAR(MAX)) AS numerisk_vanster,
		CAST(STYRD_ID AS VARCHAR(MAX)) AS styrd_id,
		CAST(STYRD_NR AS VARCHAR(MAX)) AS styrd_nr,
		CAST(URVAL AS VARCHAR(MAX)) AS urval 
	FROM utdata.utdata295.EK_SAMBAND_STYRDA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

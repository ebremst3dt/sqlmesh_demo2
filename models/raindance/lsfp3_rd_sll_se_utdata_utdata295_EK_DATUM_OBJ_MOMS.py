
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'MOMS_DATUM_FOM': 'datetime',
 'MOMS_DATUM_TOM': 'datetime',
 'MOMS_GILTIG_FOM': 'datetime',
 'MOMS_GILTIG_TOM': 'datetime',
 'MOMS_ID': 'varchar(3)',
 'MOMS_ID_TEXT': 'varchar(34)',
 'MOMS_PASSIV': 'bit',
 'MOMS_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), MOMS_DATUM_FOM, 126) AS moms_datum_fom,
		CONVERT(varchar(max), MOMS_DATUM_TOM, 126) AS moms_datum_tom,
		CONVERT(varchar(max), MOMS_GILTIG_FOM, 126) AS moms_giltig_fom,
		CONVERT(varchar(max), MOMS_GILTIG_TOM, 126) AS moms_giltig_tom,
		CAST(MOMS_ID AS VARCHAR(MAX)) AS moms_id,
		CAST(MOMS_ID_TEXT AS VARCHAR(MAX)) AS moms_id_text,
		CAST(MOMS_PASSIV AS VARCHAR(MAX)) AS moms_passiv,
		CAST(MOMS_TEXT AS VARCHAR(MAX)) AS moms_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_MOMS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

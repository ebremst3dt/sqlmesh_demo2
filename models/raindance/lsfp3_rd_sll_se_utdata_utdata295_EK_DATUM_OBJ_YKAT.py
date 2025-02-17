
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'YKAT_DATUM_FOM': 'datetime',
 'YKAT_DATUM_TOM': 'datetime',
 'YKAT_GILTIG_FOM': 'datetime',
 'YKAT_GILTIG_TOM': 'datetime',
 'YKAT_ID': 'varchar(4)',
 'YKAT_ID_TEXT': 'varchar(35)',
 'YKAT_PASSIV': 'bit',
 'YKAT_TEXT': 'varchar(30)'},
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
 		CAST(YKAT_DATUM_FOM AS VARCHAR(MAX)) AS ykat_datum_fom,
		CAST(YKAT_DATUM_TOM AS VARCHAR(MAX)) AS ykat_datum_tom,
		CAST(YKAT_GILTIG_FOM AS VARCHAR(MAX)) AS ykat_giltig_fom,
		CAST(YKAT_GILTIG_TOM AS VARCHAR(MAX)) AS ykat_giltig_tom,
		CAST(YKAT_ID AS VARCHAR(MAX)) AS ykat_id,
		CAST(YKAT_ID_TEXT AS VARCHAR(MAX)) AS ykat_id_text,
		CAST(YKAT_PASSIV AS VARCHAR(MAX)) AS ykat_passiv,
		CAST(YKAT_TEXT AS VARCHAR(MAX)) AS ykat_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_YKAT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

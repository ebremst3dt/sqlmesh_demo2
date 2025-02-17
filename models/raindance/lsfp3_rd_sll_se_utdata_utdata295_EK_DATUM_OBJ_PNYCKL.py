
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PNYCKL_DATUM_FOM': 'datetime',
 'PNYCKL_DATUM_TOM': 'datetime',
 'PNYCKL_GILTIG_FOM': 'datetime',
 'PNYCKL_GILTIG_TOM': 'datetime',
 'PNYCKL_ID': 'varchar(6)',
 'PNYCKL_ID_TEXT': 'varchar(37)',
 'PNYCKL_PASSIV': 'bit',
 'PNYCKL_TEXT': 'varchar(30)'},
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
 		CAST(PNYCKL_DATUM_FOM AS VARCHAR(MAX)) AS pnyckl_datum_fom,
		CAST(PNYCKL_DATUM_TOM AS VARCHAR(MAX)) AS pnyckl_datum_tom,
		CAST(PNYCKL_GILTIG_FOM AS VARCHAR(MAX)) AS pnyckl_giltig_fom,
		CAST(PNYCKL_GILTIG_TOM AS VARCHAR(MAX)) AS pnyckl_giltig_tom,
		CAST(PNYCKL_ID AS VARCHAR(MAX)) AS pnyckl_id,
		CAST(PNYCKL_ID_TEXT AS VARCHAR(MAX)) AS pnyckl_id_text,
		CAST(PNYCKL_PASSIV AS VARCHAR(MAX)) AS pnyckl_passiv,
		CAST(PNYCKL_TEXT AS VARCHAR(MAX)) AS pnyckl_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_PNYCKL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

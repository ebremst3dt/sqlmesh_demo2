
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOTYP_DATUM_FOM': 'varchar(max)',
 'BOTYP_DATUM_TOM': 'varchar(max)',
 'BOTYP_GILTIG_FOM': 'varchar(max)',
 'BOTYP_GILTIG_TOM': 'varchar(max)',
 'BOTYP_ID': 'varchar(max)',
 'BOTYP_ID_TEXT': 'varchar(max)',
 'BOTYP_PASSIV': 'varchar(max)',
 'BOTYP_TEXT': 'varchar(max)'},
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
 		CAST(BOTYP_DATUM_FOM AS VARCHAR(MAX)) AS botyp_datum_fom,
		CAST(BOTYP_DATUM_TOM AS VARCHAR(MAX)) AS botyp_datum_tom,
		CAST(BOTYP_GILTIG_FOM AS VARCHAR(MAX)) AS botyp_giltig_fom,
		CAST(BOTYP_GILTIG_TOM AS VARCHAR(MAX)) AS botyp_giltig_tom,
		CAST(BOTYP_ID AS VARCHAR(MAX)) AS botyp_id,
		CAST(BOTYP_ID_TEXT AS VARCHAR(MAX)) AS botyp_id_text,
		CAST(BOTYP_PASSIV AS VARCHAR(MAX)) AS botyp_passiv,
		CAST(BOTYP_TEXT AS VARCHAR(MAX)) AS botyp_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BOTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

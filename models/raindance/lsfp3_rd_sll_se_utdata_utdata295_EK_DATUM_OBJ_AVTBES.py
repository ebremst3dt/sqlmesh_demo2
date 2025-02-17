
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'AVTBES_DATUM_FOM': 'varchar(max)',
 'AVTBES_DATUM_TOM': 'varchar(max)',
 'AVTBES_GILTIG_FOM': 'varchar(max)',
 'AVTBES_GILTIG_TOM': 'varchar(max)',
 'AVTBES_ID': 'varchar(max)',
 'AVTBES_ID_TEXT': 'varchar(max)',
 'AVTBES_PASSIV': 'varchar(max)',
 'AVTBES_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
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
 		CAST(AVTBES_DATUM_FOM AS VARCHAR(MAX)) AS avtbes_datum_fom,
		CAST(AVTBES_DATUM_TOM AS VARCHAR(MAX)) AS avtbes_datum_tom,
		CAST(AVTBES_GILTIG_FOM AS VARCHAR(MAX)) AS avtbes_giltig_fom,
		CAST(AVTBES_GILTIG_TOM AS VARCHAR(MAX)) AS avtbes_giltig_tom,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS avtbes_id_text,
		CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS avtbes_passiv,
		CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS avtbes_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_AVTBES
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

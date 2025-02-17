
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'FRI1_DATUM_FOM': 'varchar(max)',
 'FRI1_DATUM_TOM': 'varchar(max)',
 'FRI1_GILTIG_FOM': 'varchar(max)',
 'FRI1_GILTIG_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI1_ID_TEXT': 'varchar(max)',
 'FRI1_PASSIV': 'varchar(max)',
 'FRI1_TEXT': 'varchar(max)'},
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
 		CAST(FRI1_DATUM_FOM AS VARCHAR(MAX)) AS fri1_datum_fom,
		CAST(FRI1_DATUM_TOM AS VARCHAR(MAX)) AS fri1_datum_tom,
		CAST(FRI1_GILTIG_FOM AS VARCHAR(MAX)) AS fri1_giltig_fom,
		CAST(FRI1_GILTIG_TOM AS VARCHAR(MAX)) AS fri1_giltig_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI1_ID_TEXT AS VARCHAR(MAX)) AS fri1_id_text,
		CAST(FRI1_PASSIV AS VARCHAR(MAX)) AS fri1_passiv,
		CAST(FRI1_TEXT AS VARCHAR(MAX)) AS fri1_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_FRI1
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

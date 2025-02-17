
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'EJITABELL_ID': 'varchar(1)',
 'EJITABELL_NR': 'numeric',
 'INVIA_ID': 'varchar(6)',
 'INVIA_NR': 'numeric',
 'OBJTYPLANGD': 'numeric',
 'OBJTYP_ID': 'varchar(6)',
 'OBJTYP_ID_TEXT': 'varchar(38)',
 'OBJTYP_NR': 'numeric',
 'OBJTYP_TEXT': 'varchar(20)'},
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
 		CAST(EJITABELL_ID AS VARCHAR(MAX)) AS ejitabell_id,
		CAST(EJITABELL_NR AS VARCHAR(MAX)) AS ejitabell_nr,
		CAST(INVIA_ID AS VARCHAR(MAX)) AS invia_id,
		CAST(INVIA_NR AS VARCHAR(MAX)) AS invia_nr,
		CAST(OBJTYP_ID AS VARCHAR(MAX)) AS objtyp_id,
		CAST(OBJTYP_ID_TEXT AS VARCHAR(MAX)) AS objtyp_id_text,
		CAST(OBJTYP_NR AS VARCHAR(MAX)) AS objtyp_nr,
		CAST(OBJTYP_TEXT AS VARCHAR(MAX)) AS objtyp_text,
		CAST(OBJTYPLANGD AS VARCHAR(MAX)) AS objtyplangd 
	FROM utdata.utdata295.EK_OBJTYPER
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

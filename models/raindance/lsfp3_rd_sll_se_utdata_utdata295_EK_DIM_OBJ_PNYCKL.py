
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'PNYCKL_GILTIG_FOM': 'varchar(max)',
 'PNYCKL_GILTIG_TOM': 'varchar(max)',
 'PNYCKL_ID': 'varchar(max)',
 'PNYCKL_ID_TEXT': 'varchar(max)',
 'PNYCKL_PASSIV': 'varchar(max)',
 'PNYCKL_TEXT': 'varchar(max)'},
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
 		CAST(PNYCKL_GILTIG_FOM AS VARCHAR(MAX)) AS pnyckl_giltig_fom,
		CAST(PNYCKL_GILTIG_TOM AS VARCHAR(MAX)) AS pnyckl_giltig_tom,
		CAST(PNYCKL_ID AS VARCHAR(MAX)) AS pnyckl_id,
		CAST(PNYCKL_ID_TEXT AS VARCHAR(MAX)) AS pnyckl_id_text,
		CAST(PNYCKL_PASSIV AS VARCHAR(MAX)) AS pnyckl_passiv,
		CAST(PNYCKL_TEXT AS VARCHAR(MAX)) AS pnyckl_text 
	FROM utdata.utdata295.EK_DIM_OBJ_PNYCKL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

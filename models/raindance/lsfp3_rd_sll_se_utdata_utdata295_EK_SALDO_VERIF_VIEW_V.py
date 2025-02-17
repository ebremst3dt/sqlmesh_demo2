
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_ID': 'varchar(7)',
 'FRI1_ID': 'varchar(4)',
 'FRI2_ID': 'varchar(3)',
 'KONTO_ID': 'varchar(4)',
 'KST_ID': 'varchar(5)',
 'MOTP_ID': 'varchar(4)',
 'PROJ_ID': 'varchar(5)',
 'UTFALL_V': 'numeric',
 'VERK_ID': 'varchar(2)'},
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
 		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id 
	FROM utdata.utdata295.EK_SALDO_VERIF_VIEW_V
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

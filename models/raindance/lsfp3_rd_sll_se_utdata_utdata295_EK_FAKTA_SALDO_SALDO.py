
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_ID': 'varchar(max)',
 'BFAR': 'varchar(max)',
 'BUDGET_ACK': 'varchar(max)',
 'BUDGET_AR': 'varchar(max)',
 'BUDGET_FGPER': 'varchar(max)',
 'BUDGET_PER': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'PERIOD': 'varchar(max)',
 'PRG_FGPER': 'varchar(max)',
 'PRG_PER': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'UTFALL_ACK': 'varchar(max)',
 'UTFALL_AR': 'varchar(max)',
 'UTFALL_FGPER': 'varchar(max)',
 'UTFALL_IB': 'varchar(max)',
 'UTFALL_PER': 'varchar(max)',
 'VERK_ID': 'varchar(max)'},
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
	SELECT TOP 1000 top 1000
 		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CONVERT(varchar(max), BFAR, 126) AS bfar,
		CAST(BUDGET_ACK AS VARCHAR(MAX)) AS budget_ack,
		CAST(BUDGET_AR AS VARCHAR(MAX)) AS budget_ar,
		CAST(BUDGET_FGPER AS VARCHAR(MAX)) AS budget_fgper,
		CAST(BUDGET_PER AS VARCHAR(MAX)) AS budget_per,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CONVERT(varchar(max), PERIOD, 126) AS period,
		CAST(PRG_FGPER AS VARCHAR(MAX)) AS prg_fgper,
		CAST(PRG_PER AS VARCHAR(MAX)) AS prg_per,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(UTFALL_ACK AS VARCHAR(MAX)) AS utfall_ack,
		CAST(UTFALL_AR AS VARCHAR(MAX)) AS utfall_ar,
		CAST(UTFALL_FGPER AS VARCHAR(MAX)) AS utfall_fgper,
		CAST(UTFALL_IB AS VARCHAR(MAX)) AS utfall_ib,
		CAST(UTFALL_PER AS VARCHAR(MAX)) AS utfall_per,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id 
	FROM utdata.utdata295.EK_FAKTA_SALDO_SALDO
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

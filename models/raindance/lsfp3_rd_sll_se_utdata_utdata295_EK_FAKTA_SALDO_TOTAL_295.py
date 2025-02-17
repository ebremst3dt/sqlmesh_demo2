
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_ID': 'varchar(7)',
 'BFAR': 'datetime',
 'BUDGET_ACK': 'numeric',
 'BUDGET_AR': 'numeric',
 'BUDGET_ARSACK': 'numeric',
 'BUDGET_FGACK': 'numeric',
 'BUDGET_FGAR': 'numeric',
 'BUDGET_FGPER': 'numeric',
 'BUDGET_PER': 'numeric',
 'FRI1_ID': 'varchar(4)',
 'FRI2_ID': 'varchar(3)',
 'KONTO_ID': 'varchar(4)',
 'KST_ID': 'varchar(5)',
 'LFRAM_AR': 'numeric',
 'LFRAM_PER': 'numeric',
 'MOTP_ID': 'varchar(4)',
 'PERIOD': 'datetime',
 'PRG_FGPER': 'numeric',
 'PRG_PER': 'numeric',
 'PROJ_ID': 'varchar(5)',
 'UTFALL_ACK': 'numeric',
 'UTFALL_AR': 'numeric',
 'UTFALL_ARSACK': 'numeric',
 'UTFALL_FGACK': 'numeric',
 'UTFALL_FGAR': 'numeric',
 'UTFALL_FGPER': 'numeric',
 'UTFALL_IB': 'numeric',
 'UTFALL_PER': 'numeric',
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
		CONVERT(varchar(max), BFAR, 126) AS bfar,
		CAST(BUDGET_ACK AS VARCHAR(MAX)) AS budget_ack,
		CAST(BUDGET_AR AS VARCHAR(MAX)) AS budget_ar,
		CAST(BUDGET_ARSACK AS VARCHAR(MAX)) AS budget_arsack,
		CAST(BUDGET_FGACK AS VARCHAR(MAX)) AS budget_fgack,
		CAST(BUDGET_FGAR AS VARCHAR(MAX)) AS budget_fgar,
		CAST(BUDGET_FGPER AS VARCHAR(MAX)) AS budget_fgper,
		CAST(BUDGET_PER AS VARCHAR(MAX)) AS budget_per,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LFRAM_AR AS VARCHAR(MAX)) AS lfram_ar,
		CAST(LFRAM_PER AS VARCHAR(MAX)) AS lfram_per,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CONVERT(varchar(max), PERIOD, 126) AS period,
		CAST(PRG_FGPER AS VARCHAR(MAX)) AS prg_fgper,
		CAST(PRG_PER AS VARCHAR(MAX)) AS prg_per,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(UTFALL_ACK AS VARCHAR(MAX)) AS utfall_ack,
		CAST(UTFALL_AR AS VARCHAR(MAX)) AS utfall_ar,
		CAST(UTFALL_ARSACK AS VARCHAR(MAX)) AS utfall_arsack,
		CAST(UTFALL_FGACK AS VARCHAR(MAX)) AS utfall_fgack,
		CAST(UTFALL_FGAR AS VARCHAR(MAX)) AS utfall_fgar,
		CAST(UTFALL_FGPER AS VARCHAR(MAX)) AS utfall_fgper,
		CAST(UTFALL_IB AS VARCHAR(MAX)) AS utfall_ib,
		CAST(UTFALL_PER AS VARCHAR(MAX)) AS utfall_per,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id 
	FROM utdata.utdata295.EK_FAKTA_SALDO_TOTAL_295
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

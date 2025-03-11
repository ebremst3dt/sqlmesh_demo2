
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'AVTBES_ID': 'varchar(max)', 'BFAR': 'varchar(max)', 'BUDGET_ACK': 'varchar(max)', 'BUDGET_AR': 'varchar(max)', 'BUDGET_ARSACK': 'varchar(max)', 'BUDGET_FGACK': 'varchar(max)', 'BUDGET_FGAR': 'varchar(max)', 'BUDGET_FGPER': 'varchar(max)', 'BUDGET_PER': 'varchar(max)', 'FRI1_ID': 'varchar(max)', 'FRI2_ID': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LFRAM_AR': 'varchar(max)', 'LFRAM_PER': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'PERIOD': 'varchar(max)', 'PRG_FGPER': 'varchar(max)', 'PRG_PER': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'UTFALL_ACK': 'varchar(max)', 'UTFALL_AR': 'varchar(max)', 'UTFALL_ARSACK': 'varchar(max)', 'UTFALL_FGACK': 'varchar(max)', 'UTFALL_FGAR': 'varchar(max)', 'UTFALL_FGPER': 'varchar(max)', 'UTFALL_IB': 'varchar(max)', 'UTFALL_PER': 'varchar(max)', 'VERK_ID': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
    ),
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
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
	FROM utdata.utdata295.EK_FAKTA_SALDO_TOTAL_295) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
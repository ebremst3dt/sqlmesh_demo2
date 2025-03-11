
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTBES_ID': 'varchar(max)', 'BPUTF_V': 'varchar(max)', 'BUDGET_01': 'varchar(max)', 'BUDGET_02': 'varchar(max)', 'BUDGET_03': 'varchar(max)', 'BUDGET_04': 'varchar(max)', 'BUDGET_05': 'varchar(max)', 'BUDGET_06': 'varchar(max)', 'BUDGET_07': 'varchar(max)', 'BUDGET_08': 'varchar(max)', 'BUDGET_09': 'varchar(max)', 'BUDGET_10': 'varchar(max)', 'BUDGET_11': 'varchar(max)', 'BUDGET_12': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FRI1_ID': 'varchar(max)', 'FRI2_ID': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'IB': 'varchar(max)', 'ID_ID': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LFRAM_V': 'varchar(max)', 'MED': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PRG_V': 'varchar(max)', 'PROC_ID': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGDAT_ID': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'STATUS': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'URS_ID': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'YKAT_ID': 'varchar(max)'},
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
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS bputf_v,
		CAST(BUDGET_01 AS VARCHAR(MAX)) AS budget_01,
		CAST(BUDGET_02 AS VARCHAR(MAX)) AS budget_02,
		CAST(BUDGET_03 AS VARCHAR(MAX)) AS budget_03,
		CAST(BUDGET_04 AS VARCHAR(MAX)) AS budget_04,
		CAST(BUDGET_05 AS VARCHAR(MAX)) AS budget_05,
		CAST(BUDGET_06 AS VARCHAR(MAX)) AS budget_06,
		CAST(BUDGET_07 AS VARCHAR(MAX)) AS budget_07,
		CAST(BUDGET_08 AS VARCHAR(MAX)) AS budget_08,
		CAST(BUDGET_09 AS VARCHAR(MAX)) AS budget_09,
		CAST(BUDGET_10 AS VARCHAR(MAX)) AS budget_10,
		CAST(BUDGET_11 AS VARCHAR(MAX)) AS budget_11,
		CAST(BUDGET_12 AS VARCHAR(MAX)) AS budget_12,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS budget_v,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
		CAST(FORETAG AS VARCHAR(MAX)) AS foretag,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS föproc_id,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS förbel_v,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(ID_ID AS VARCHAR(MAX)) AS id_id,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LFRAM_V AS VARCHAR(MAX)) AS lfram_v,
		CAST(MED AS VARCHAR(MAX)) AS med,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS orgval_v,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS pnyckel,
		CAST(PRG_V AS VARCHAR(MAX)) AS prg_v,
		CAST(PROC_ID AS VARCHAR(MAX)) AS proc_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RADTEXT AS VARCHAR(MAX)) AS radtext,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS radtypnr,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS regdat_id,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(YKAT_ID AS VARCHAR(MAX)) AS ykat_id 
	FROM utdata.utdata295.EK_FAKTA_VERIFIKAT2_BUDGET

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
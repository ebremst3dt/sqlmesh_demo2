
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ATTESTDATUM1': 'datetime',
 'ATTESTDATUM2': 'datetime',
 'ATTESTSIGN1': 'varchar(3)',
 'ATTESTSIGN2': 'varchar(3)',
 'AVTBES_ID': 'varchar(7)',
 'BPUTF_V': 'numeric',
 'BUDGET_V': 'numeric',
 'DEFDATUM': 'datetime',
 'DEFSIGN': 'varchar(3)',
 'DOKTYP': 'numeric',
 'DOKUMENTID': 'varchar(20)',
 'DOK_ANTAL': 'numeric',
 'EXTERNANM': 'varchar(80)',
 'EXTERNID': 'varchar(19)',
 'EXTERNNR': 'varchar(20)',
 'FRI1_ID': 'varchar(4)',
 'FRI2_ID': 'varchar(3)',
 'FÖPROC_ID': 'varchar(4)',
 'FÖRBEL_V': 'numeric',
 'HUVUDTEXT': 'varchar(30)',
 'IB': 'varchar(1)',
 'ID_ID': 'varchar(16)',
 'INTERNVERNR': 'numeric',
 'KATEGORI': 'numeric',
 'KONTO_ID': 'varchar(4)',
 'KONTSIGN': 'varchar(3)',
 'KST_ID': 'varchar(5)',
 'LFRAM_V': 'numeric',
 'MED': 'numeric',
 'MOTP_ID': 'varchar(4)',
 'ORGVAL_V': 'numeric',
 'PNYCKEL': 'varchar(10)',
 'PRG_V': 'numeric',
 'PROC_ID': 'varchar(1)',
 'PROJ_ID': 'varchar(5)',
 'RADTEXT': 'varchar(30)',
 'RADTYPNR': 'numeric',
 'REGDATUM': 'datetime',
 'REGDAT_ID': 'varchar(20)',
 'REGSIGN': 'varchar(3)',
 'STATUS': 'varchar(1)',
 'URSPTEXT': 'varchar(30)',
 'URS_ID': 'varchar(2)',
 'UTFALL_V': 'numeric',
 'UTILITY': 'numeric',
 'VALUTA_ID': 'varchar(3)',
 'VERDATUM': 'datetime',
 'VERDOKREF': 'varchar(200)',
 'VERK_ID': 'varchar(2)',
 'VERNR': 'numeric',
 'VERRAD': 'numeric',
 'VERTYP': 'varchar(6)',
 'YKAT_ID': 'varchar(4)'},
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
 		CAST(ATTESTDATUM1 AS VARCHAR(MAX)) AS attestdatum1,
		CAST(ATTESTDATUM2 AS VARCHAR(MAX)) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS bputf_v,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS budget_v,
		CAST(DEFDATUM AS VARCHAR(MAX)) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
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
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS regdat_id,
		CAST(REGDATUM AS VARCHAR(MAX)) AS regdatum,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(YKAT_ID AS VARCHAR(MAX)) AS ykat_id 
	FROM utdata.utdata295.EK_FAKTA_VERIFIKAT_295
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

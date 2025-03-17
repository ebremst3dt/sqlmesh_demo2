
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AKT_ID': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVT_ID': 'varchar(max)', 'BESTUT_V': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'BUDV2_V': 'varchar(max)', 'DATUM_ID': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'ERS_ID': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'HÄND_ID': 'varchar(max)', 'IB': 'varchar(max)', 'IMOT_ID': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KASSA_ID': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KST_ID': 'varchar(max)', 'MED': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROG_V': 'varchar(max)', 'PROR12_V': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGDAT_ID': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SID_ID': 'varchar(max)', 'STATUS': 'varchar(max)', 'TRID_ID': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTFUTL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VAL_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'YRK_ID': 'varchar(max)'},
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
    query = f"""
	SELECT * FROM (SELECT 
 		CAST(CAST(VERDATUM AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CAST(AKT_ID AS VARCHAR(MAX)) AS akt_id,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVT_ID AS VARCHAR(MAX)) AS avt_id,
		CAST(BESTUT_V AS VARCHAR(MAX)) AS bestut_v,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS budget_v,
		CAST(BUDV2_V AS VARCHAR(MAX)) AS budv2_v,
		CAST(DATUM_ID AS VARCHAR(MAX)) AS datum_id,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(ERS_ID AS VARCHAR(MAX)) AS ers_id,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
		CAST(FORETAG AS VARCHAR(MAX)) AS foretag,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS föproc_id,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS förbel_v,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS händ_id,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(IMOT_ID AS VARCHAR(MAX)) AS imot_id,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS kassa_id,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MED AS VARCHAR(MAX)) AS med,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS orgval_v,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS pnyckel,
		CAST(PROG_V AS VARCHAR(MAX)) AS prog_v,
		CAST(PROR12_V AS VARCHAR(MAX)) AS pror12_v,
		CAST(RADTEXT AS VARCHAR(MAX)) AS radtext,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS radtypnr,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS regdat_id,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(SID_ID AS VARCHAR(MAX)) AS sid_id,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(TRID_ID AS VARCHAR(MAX)) AS trid_id,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTFUTL_V AS VARCHAR(MAX)) AS utfutl_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VAL_ID AS VARCHAR(MAX)) AS val_id,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(YRK_ID AS VARCHAR(MAX)) AS yrk_id 
	FROM udpb4.udpb4_100.EK_FAKTA_VERIFIKAT ) y
WHERE _data_modified_utc between DATEADD(month, -3, GETDATE()) and GETDATE()
	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTAL_ID': 'varchar(max)', 'BOK_V': 'varchar(max)', 'BPUTF_V': 'varchar(max)', 'BUD_V': 'varchar(max)', 'DEFANL_ID': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FAKT_ID': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'HÄND_ID': 'varchar(max)', 'IB': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KASSA_ID': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LEVID_ID': 'varchar(max)', 'MED': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MPAYID_ID': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'STATUS': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPR_ID': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTF_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'XFAKT_ID': 'varchar(max)', 'XLEVID_ID': 'varchar(max)', 'YG_ID': 'varchar(max)'},
    
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=5000,
        time_column="_data_modified_utc"
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS avtal_id,
		CAST(BOK_V AS VARCHAR(MAX)) AS bok_v,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS bputf_v,
		CAST(BUD_V AS VARCHAR(MAX)) AS bud_v,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS defanl_id,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
		CAST(FAKT_ID AS VARCHAR(MAX)) AS fakt_id,
		CAST(FORETAG AS VARCHAR(MAX)) AS foretag,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS föproc_id,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS förbel_v,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS händ_id,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS kassa_id,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LEVID_ID AS VARCHAR(MAX)) AS levid_id,
		CAST(MED AS VARCHAR(MAX)) AS med,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(MPAYID_ID AS VARCHAR(MAX)) AS mpayid_id,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS orgval_v,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS pnyckel,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RADTEXT AS VARCHAR(MAX)) AS radtext,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS radtypnr,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS urspr_id,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(UTF_V AS VARCHAR(MAX)) AS utf_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(XFAKT_ID AS VARCHAR(MAX)) AS xfakt_id,
		CAST(XLEVID_ID AS VARCHAR(MAX)) AS xlevid_id,
		CAST(YG_ID AS VARCHAR(MAX)) AS yg_id 
	FROM raindance_udp.udp_150.EK_FAKTA_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    
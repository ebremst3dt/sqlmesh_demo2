
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
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS ATTESTDATUM1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS ATTESTDATUM2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS AVTAL_ID,
		CAST(BOK_V AS VARCHAR(MAX)) AS BOK_V,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS BPUTF_V,
		CAST(BUD_V AS VARCHAR(MAX)) AS BUD_V,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS DEFANL_ID,
		CONVERT(varchar(max), DEFDATUM, 126) AS DEFDATUM,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
		CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS DOK_ANTAL,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS EXTERNANM,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS EXTERNNR,
		CAST(FAKT_ID AS VARCHAR(MAX)) AS FAKT_ID,
		CAST(FORETAG AS VARCHAR(MAX)) AS FORETAG,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS FÖRBEL_V,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS HÄND_ID,
		CAST(IB AS VARCHAR(MAX)) AS IB,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS KASSA_ID,
		CAST(KATEGORI AS VARCHAR(MAX)) AS KATEGORI,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS KONTSIGN,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(LEVID_ID AS VARCHAR(MAX)) AS LEVID_ID,
		CAST(MED AS VARCHAR(MAX)) AS MED,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MPAYID_ID AS VARCHAR(MAX)) AS MPAYID_ID,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS ORGVAL_V,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(RADTEXT AS VARCHAR(MAX)) AS RADTEXT,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS RADTYPNR,
		CONVERT(varchar(max), REGDATUM, 126) AS REGDATUM,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS URSPRUNGS_VERIFIKAT,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS URSPR_ID,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS URSPTEXT,
		CAST(UTF_V AS VARCHAR(MAX)) AS UTF_V,
		CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS VALUTA_ID,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS VERDOKREF,
		CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
		CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD,
		CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
		CAST(XFAKT_ID AS VARCHAR(MAX)) AS XFAKT_ID,
		CAST(XLEVID_ID AS VARCHAR(MAX)) AS XLEVID_ID,
		CAST(YG_ID AS VARCHAR(MAX)) AS YG_ID 
	FROM raindance_udp.udp_150.EK_FAKTA_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    
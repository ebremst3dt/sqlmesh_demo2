
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AKT_ID': 'varchar(max)', 'ANTAL_V': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTAL_ID': 'varchar(max)', 'BPUTF_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HDATUM_ID': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'IB': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'INVNR_ID': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LEVID_ID': 'varchar(max)', 'MED': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'OKI_ID': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROD_ID': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'RAD_ID': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SKI_ID': 'varchar(max)', 'STATUS': 'varchar(max)', 'TANV_ID': 'varchar(max)', 'TIMMAR_V': 'varchar(max)', 'TTYP_ID': 'varchar(max)', 'TYP_ID': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPR_ID': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'YRKE_ID': 'varchar(max)'},
    
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CAST(AKT_ID AS VARCHAR(MAX)) AS AKT_ID,
		CAST(ANTAL_V AS VARCHAR(MAX)) AS ANTAL_V,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS ATTESTDATUM1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS ATTESTDATUM2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS AVTAL_ID,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS BPUTF_V,
		CONVERT(varchar(max), DEFDATUM, 126) AS DEFDATUM,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
		CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS DOK_ANTAL,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS EXTERNANM,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS EXTERNNR,
		CAST(FORETAG AS VARCHAR(MAX)) AS FORETAG,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS FÖRBEL_V,
		CAST(HDATUM_ID AS VARCHAR(MAX)) AS HDATUM_ID,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
		CAST(IB AS VARCHAR(MAX)) AS IB,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(INVNR_ID AS VARCHAR(MAX)) AS INVNR_ID,
		CAST(KATEGORI AS VARCHAR(MAX)) AS KATEGORI,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS KONTSIGN,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(LEVID_ID AS VARCHAR(MAX)) AS LEVID_ID,
		CAST(MED AS VARCHAR(MAX)) AS MED,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(OKI_ID AS VARCHAR(MAX)) AS OKI_ID,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS ORGVAL_V,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
		CAST(PROD_ID AS VARCHAR(MAX)) AS PROD_ID,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(RADTEXT AS VARCHAR(MAX)) AS RADTEXT,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS RADTYPNR,
		CAST(RAD_ID AS VARCHAR(MAX)) AS RAD_ID,
		CONVERT(varchar(max), REGDATUM, 126) AS REGDATUM,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CAST(SKI_ID AS VARCHAR(MAX)) AS SKI_ID,
		CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
		CAST(TANV_ID AS VARCHAR(MAX)) AS TANV_ID,
		CAST(TIMMAR_V AS VARCHAR(MAX)) AS TIMMAR_V,
		CAST(TTYP_ID AS VARCHAR(MAX)) AS TTYP_ID,
		CAST(TYP_ID AS VARCHAR(MAX)) AS TYP_ID,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS URSPRUNGS_VERIFIKAT,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS URSPR_ID,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS URSPTEXT,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
		CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS VALUTA_ID,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS VERDOKREF,
		CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
		CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD,
		CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
		CAST(YRKE_ID AS VARCHAR(MAX)) AS YRKE_ID 
	FROM Utdata.udp_100.EK_FAKTA_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    
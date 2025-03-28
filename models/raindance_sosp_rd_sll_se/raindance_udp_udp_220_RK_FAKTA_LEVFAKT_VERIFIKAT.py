
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTEST': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTALID': 'varchar(max)', 'AVTAL_ID': 'varchar(max)', 'BELOPP_SEK': 'varchar(max)', 'BELOPP_VAL': 'varchar(max)', 'BETALNINGSSPARR': 'varchar(max)', 'BETALT_SEK': 'varchar(max)', 'BETALT_VAL': 'varchar(max)', 'BETDAGAR': 'varchar(max)', 'BPUTF_V': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'DAVST_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EREF': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FAKPERSLUTDAT': 'varchar(max)', 'FAKPERSTARTDAT': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTURADATUM': 'varchar(max)', 'FAKTURA_REGDATUM': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FORFALLODATUM': 'varchar(max)', 'F_DOK_ANTAL': 'varchar(max)', 'F_MED': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'HÄND_ID': 'varchar(max)', 'IB': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KASSA_ID': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KAVST_V': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KRAVNIVA': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVRTYP': 'varchar(max)', 'MED': 'varchar(max)', 'MOMS_SEK': 'varchar(max)', 'MOMS_VAL': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTTATTDAT': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'NR': 'varchar(max)', 'OCRNR': 'varchar(max)', 'OVERDRDAGAR': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'RANTEDEB': 'varchar(max)', 'RAPPD_ID': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'STATUS': 'varchar(max)', 'TAB_ATTEST': 'varchar(max)', 'TAB_BESTNR': 'varchar(max)', 'TAB_BETV': 'varchar(max)', 'TAB_BMOMS': 'varchar(max)', 'TAB_C2': 'varchar(max)', 'TAB_CMALL': 'varchar(max)', 'TAB_CW': 'varchar(max)', 'TAB_LAND': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_RESK': 'varchar(max)', 'TAB_RKST': 'varchar(max)', 'TAB_UBF': 'varchar(max)', 'TAB_UBK': 'varchar(max)', 'TAB_VALUTA': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPR_ID': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'UTSKRDATUM': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VALUTA_V': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'VREF': 'varchar(max)', 'YRKG_ID': 'varchar(max)'},
    
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CAST(ATTEST AS VARCHAR(MAX)) AS ATTEST,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS ATTESTDATUM1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS ATTESTDATUM2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(AVTALID AS VARCHAR(MAX)) AS AVTALID,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS AVTAL_ID,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS BELOPP_SEK,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS BELOPP_VAL,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS BETALNINGSSPARR,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS BETALT_SEK,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS BETALT_VAL,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS BETDAGAR,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS BPUTF_V,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS BUDGET_V,
		CAST(DAVST_V AS VARCHAR(MAX)) AS DAVST_V,
		CONVERT(varchar(max), DEFDATUM, 126) AS DEFDATUM,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
		CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS DOK_ANTAL,
		CAST(EREF AS VARCHAR(MAX)) AS EREF,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS EXTERNANM,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS EXTERNNR,
		CONVERT(varchar(max), FAKPERSLUTDAT, 126) AS FAKPERSLUTDAT,
		CONVERT(varchar(max), FAKPERSTARTDAT, 126) AS FAKPERSTARTDAT,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS FAKTSTATUS,
		CONVERT(varchar(max), FAKTURADATUM, 126) AS FAKTURADATUM,
		CONVERT(varchar(max), FAKTURA_REGDATUM, 126) AS FAKTURA_REGDATUM,
		CAST(FORETAG AS VARCHAR(MAX)) AS FORETAG,
		CONVERT(varchar(max), FORFALLODATUM, 126) AS FORFALLODATUM,
		CAST(F_DOK_ANTAL AS VARCHAR(MAX)) AS F_DOK_ANTAL,
		CAST(F_MED AS VARCHAR(MAX)) AS F_MED,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS FÖRBEL_V,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS HÄND_ID,
		CAST(IB AS VARCHAR(MAX)) AS IB,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS KASSA_ID,
		CAST(KATEGORI AS VARCHAR(MAX)) AS KATEGORI,
		CAST(KAVST_V AS VARCHAR(MAX)) AS KAVST_V,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS KONTSIGN,
		CAST(KRAVNIVA AS VARCHAR(MAX)) AS KRAVNIVA,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(LEVID AS VARCHAR(MAX)) AS LEVID,
		CAST(LEVRTYP AS VARCHAR(MAX)) AS LEVRTYP,
		CAST(MED AS VARCHAR(MAX)) AS MED,
		CAST(MOMS_SEK AS VARCHAR(MAX)) AS MOMS_SEK,
		CAST(MOMS_VAL AS VARCHAR(MAX)) AS MOMS_VAL,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CONVERT(varchar(max), MOTTATTDAT, 126) AS MOTTATTDAT,
		CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS MOTTATTSIGN,
		CAST(NR AS VARCHAR(MAX)) AS NR,
		CAST(OCRNR AS VARCHAR(MAX)) AS OCRNR,
		CAST(OVERDRDAGAR AS VARCHAR(MAX)) AS OVERDRDAGAR,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(RADTEXT AS VARCHAR(MAX)) AS RADTEXT,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS RADTYPNR,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS RANTEDEB,
		CAST(RAPPD_ID AS VARCHAR(MAX)) AS RAPPD_ID,
		CONVERT(varchar(max), REGDATUM, 126) AS REGDATUM,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS SENASTBETDATUM,
		CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
		CAST(TAB_ATTEST AS VARCHAR(MAX)) AS TAB_ATTEST,
		CAST(TAB_BESTNR AS VARCHAR(MAX)) AS TAB_BESTNR,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS TAB_BETV,
		CAST(TAB_BMOMS AS VARCHAR(MAX)) AS TAB_BMOMS,
		CAST(TAB_C2 AS VARCHAR(MAX)) AS TAB_C2,
		CAST(TAB_CMALL AS VARCHAR(MAX)) AS TAB_CMALL,
		CAST(TAB_CW AS VARCHAR(MAX)) AS TAB_CW,
		CAST(TAB_LAND AS VARCHAR(MAX)) AS TAB_LAND,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS TAB_MOMS,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS TAB_MOTP,
		CAST(TAB_RESK AS VARCHAR(MAX)) AS TAB_RESK,
		CAST(TAB_RKST AS VARCHAR(MAX)) AS TAB_RKST,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS TAB_UBF,
		CAST(TAB_UBK AS VARCHAR(MAX)) AS TAB_UBK,
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS TAB_VALUTA,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS URSPRUNGS_VERIFIKAT,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS URSPR_ID,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS URSPTEXT,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
		CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS UTSKRDATUM,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS VALUTA_ID,
		CAST(VALUTA_V AS VARCHAR(MAX)) AS VALUTA_V,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS VERDOKREF,
		CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
		CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD,
		CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
		CAST(VREF AS VARCHAR(MAX)) AS VREF,
		CAST(YRKG_ID AS VARCHAR(MAX)) AS YRKG_ID 
	FROM raindance_udp.udp_220.RK_FAKTA_LEVFAKT_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    
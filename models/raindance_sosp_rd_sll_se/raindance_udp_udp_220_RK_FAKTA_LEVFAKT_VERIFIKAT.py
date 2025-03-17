
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
		CAST(ATTEST AS VARCHAR(MAX)) AS attest,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTALID AS VARCHAR(MAX)) AS avtalid,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS avtal_id,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS belopp_sek,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS belopp_val,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS betalningssparr,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS betalt_sek,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS betalt_val,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS betdagar,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS bputf_v,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS budget_v,
		CAST(DAVST_V AS VARCHAR(MAX)) AS davst_v,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(EREF AS VARCHAR(MAX)) AS eref,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
		CONVERT(varchar(max), FAKPERSLUTDAT, 126) AS fakperslutdat,
		CONVERT(varchar(max), FAKPERSTARTDAT, 126) AS fakperstartdat,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS faktstatus,
		CONVERT(varchar(max), FAKTURADATUM, 126) AS fakturadatum,
		CONVERT(varchar(max), FAKTURA_REGDATUM, 126) AS faktura_regdatum,
		CAST(FORETAG AS VARCHAR(MAX)) AS foretag,
		CONVERT(varchar(max), FORFALLODATUM, 126) AS forfallodatum,
		CAST(F_DOK_ANTAL AS VARCHAR(MAX)) AS f_dok_antal,
		CAST(F_MED AS VARCHAR(MAX)) AS f_med,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS föproc_id,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS förbel_v,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS händ_id,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS kassa_id,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KAVST_V AS VARCHAR(MAX)) AS kavst_v,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KRAVNIVA AS VARCHAR(MAX)) AS kravniva,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LEVID AS VARCHAR(MAX)) AS levid,
		CAST(LEVRTYP AS VARCHAR(MAX)) AS levrtyp,
		CAST(MED AS VARCHAR(MAX)) AS med,
		CAST(MOMS_SEK AS VARCHAR(MAX)) AS moms_sek,
		CAST(MOMS_VAL AS VARCHAR(MAX)) AS moms_val,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CONVERT(varchar(max), MOTTATTDAT, 126) AS mottattdat,
		CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS mottattsign,
		CAST(NR AS VARCHAR(MAX)) AS nr,
		CAST(OCRNR AS VARCHAR(MAX)) AS ocrnr,
		CAST(OVERDRDAGAR AS VARCHAR(MAX)) AS overdrdagar,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS pnyckel,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RADTEXT AS VARCHAR(MAX)) AS radtext,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS radtypnr,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS rantedeb,
		CAST(RAPPD_ID AS VARCHAR(MAX)) AS rappd_id,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS senastbetdatum,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(TAB_ATTEST AS VARCHAR(MAX)) AS tab_attest,
		CAST(TAB_BESTNR AS VARCHAR(MAX)) AS tab_bestnr,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS tab_betv,
		CAST(TAB_BMOMS AS VARCHAR(MAX)) AS tab_bmoms,
		CAST(TAB_C2 AS VARCHAR(MAX)) AS tab_c2,
		CAST(TAB_CMALL AS VARCHAR(MAX)) AS tab_cmall,
		CAST(TAB_CW AS VARCHAR(MAX)) AS tab_cw,
		CAST(TAB_LAND AS VARCHAR(MAX)) AS tab_land,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS tab_motp,
		CAST(TAB_RESK AS VARCHAR(MAX)) AS tab_resk,
		CAST(TAB_RKST AS VARCHAR(MAX)) AS tab_rkst,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS tab_ubf,
		CAST(TAB_UBK AS VARCHAR(MAX)) AS tab_ubk,
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS tab_valuta,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS urspr_id,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS utskrdatum,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CAST(VALUTA_V AS VARCHAR(MAX)) AS valuta_v,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(VREF AS VARCHAR(MAX)) AS vref,
		CAST(YRKG_ID AS VARCHAR(MAX)) AS yrkg_id 
	FROM raindance_udp.udp_220.RK_FAKTA_LEVFAKT_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTEST': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTALID': 'varchar(max)', 'BELOPP_SEK': 'varchar(max)', 'BELOPP_VAL': 'varchar(max)', 'BETALNINGSSPARR': 'varchar(max)', 'BETALT_SEK': 'varchar(max)', 'BETALT_VAL': 'varchar(max)', 'BETDAGAR': 'varchar(max)', 'BUD_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EREF': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FAKPERSLUTDAT': 'varchar(max)', 'FAKPERSTARTDAT': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTURADATUM': 'varchar(max)', 'FAKTURA_REGDATUM': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FORFALLODATUM': 'varchar(max)', 'F_DOK_ANTAL': 'varchar(max)', 'F_MED': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'IB': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KRAVNIVA': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVRTYP': 'varchar(max)', 'MED': 'varchar(max)', 'MOMS_SEK': 'varchar(max)', 'MOMS_VAL': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTTATTDAT': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'NR': 'varchar(max)', 'OCRNR': 'varchar(max)', 'OVERDRDAGAR': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROGN_V': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'RANTEDEB': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGDAT_ID': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'STATUS': 'varchar(max)', 'STÄ_ID': 'varchar(max)', 'TAB_ATTESS': 'varchar(max)', 'TAB_ATTEST': 'varchar(max)', 'TAB_BETV': 'varchar(max)', 'TAB_CMALL': 'varchar(max)', 'TAB_E': 'varchar(max)', 'TAB_INKNR': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_RECALL': 'varchar(max)', 'TAB_RESK': 'varchar(max)', 'TAB_SKDAT': 'varchar(max)', 'TAB_SKNR': 'varchar(max)', 'TAB_UBF': 'varchar(max)', 'TAB_UBK': 'varchar(max)', 'TAB_VALUTA': 'varchar(max)', 'TRAKOD_ID': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'URS_ID': 'varchar(max)', 'UTF_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'UTSKRDATUM': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'VREF': 'varchar(max)'},
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
		'mcp1_rd_sll_se_MediCarrierUDP_utdata100' as _source,
		CAST(ATTEST AS VARCHAR(MAX)) AS attest,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTALID AS VARCHAR(MAX)) AS avtalid,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS belopp_sek,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS belopp_val,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS betalningssparr,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS betalt_sek,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS betalt_val,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS betdagar,
		CAST(BUD_V AS VARCHAR(MAX)) AS bud_v,
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
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KRAVNIVA AS VARCHAR(MAX)) AS kravniva,
		CAST(KTO_ID AS VARCHAR(MAX)) AS kto_id,
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
		CAST(PROGN_V AS VARCHAR(MAX)) AS progn_v,
		CAST(RADTEXT AS VARCHAR(MAX)) AS radtext,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS radtypnr,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS rantedeb,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS regdat_id,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS senastbetdatum,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(STÄ_ID AS VARCHAR(MAX)) AS stä_id,
		CAST(TAB_ATTESS AS VARCHAR(MAX)) AS tab_attess,
		CAST(TAB_ATTEST AS VARCHAR(MAX)) AS tab_attest,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS tab_betv,
		CAST(TAB_CMALL AS VARCHAR(MAX)) AS tab_cmall,
		CAST(TAB_E AS VARCHAR(MAX)) AS tab_e,
		CAST(TAB_INKNR AS VARCHAR(MAX)) AS tab_inknr,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS tab_motp,
		CAST(TAB_RECALL AS VARCHAR(MAX)) AS tab_recall,
		CAST(TAB_RESK AS VARCHAR(MAX)) AS tab_resk,
		CAST(TAB_SKDAT AS VARCHAR(MAX)) AS tab_skdat,
		CAST(TAB_SKNR AS VARCHAR(MAX)) AS tab_sknr,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS tab_ubf,
		CAST(TAB_UBK AS VARCHAR(MAX)) AS tab_ubk,
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS tab_valuta,
		CAST(TRAKOD_ID AS VARCHAR(MAX)) AS trakod_id,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(UTF_V AS VARCHAR(MAX)) AS utf_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS utskrdatum,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(VREF AS VARCHAR(MAX)) AS vref 
	FROM MediCarrierUDP.utdata100.RK_FAKTA_LEVFAKT_VERIFIKAT ) y
WHERE _data_modified_utc between DATEADD(month, -3, GETDATE()) and GETDATE()
	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    
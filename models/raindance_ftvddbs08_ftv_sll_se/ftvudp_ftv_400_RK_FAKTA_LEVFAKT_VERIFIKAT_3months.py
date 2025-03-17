
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AFFO_ID': 'varchar(max)', 'ANTAL_V': 'varchar(max)', 'ARPINV_ID': 'varchar(max)', 'ATTEST': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTALID': 'varchar(max)', 'BELOPP_SEK': 'varchar(max)', 'BELOPP_VAL': 'varchar(max)', 'BESTUT_V': 'varchar(max)', 'BETALNINGSSPARR': 'varchar(max)', 'BETALT_SEK': 'varchar(max)', 'BETALT_VAL': 'varchar(max)', 'BETDAGAR': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'ENHET_ID': 'varchar(max)', 'EREF': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTURADATUM': 'varchar(max)', 'FAKTURA_REGDATUM': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FORFALLODATUM': 'varchar(max)', 'F_DOK_ANTAL': 'varchar(max)', 'F_MED': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'HÄNDAT_ID': 'varchar(max)', 'HÄND_ID': 'varchar(max)', 'IB': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KRAVNIVA': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVRTYP': 'varchar(max)', 'LPMALL_ID': 'varchar(max)', 'MED': 'varchar(max)', 'MOMS_SEK': 'varchar(max)', 'MOMS_VAL': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTTATTDAT': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'NR': 'varchar(max)', 'OCRNR': 'varchar(max)', 'OVERDRDAGAR': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'RAD_ID': 'varchar(max)', 'RANTEDEB': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGDAT_ID': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'RESPRO_ID': 'varchar(max)', 'RESPRO_V': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'SPEC_ID': 'varchar(max)', 'STATUS': 'varchar(max)', 'TAB_BETV': 'varchar(max)', 'TAB_BGCNR': 'varchar(max)', 'TAB_ENHET': 'varchar(max)', 'TAB_KAT': 'varchar(max)', 'TAB_LPMALL': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'TAB_MOMSOM': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_ORDNR': 'varchar(max)', 'TAB_SKDAT': 'varchar(max)', 'TAB_SKNR': 'varchar(max)', 'TAB_UBF': 'varchar(max)', 'TAB_UBK': 'varchar(max)', 'TAB_VALUTA': 'varchar(max)', 'TYP_ID': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTFVAL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'UTSKRDATUM': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'VREF': 'varchar(max)', 'YGRP_ID': 'varchar(max)'},
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
		'ftvddbs08_ftv_sll_se_ftvudp_ftv_400' as _source,
		CAST(AFFO_ID AS VARCHAR(MAX)) AS affo_id,
		CAST(ANTAL_V AS VARCHAR(MAX)) AS antal_v,
		CAST(ARPINV_ID AS VARCHAR(MAX)) AS arpinv_id,
		CAST(ATTEST AS VARCHAR(MAX)) AS attest,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS attestdatum1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS attestdatum2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(AVTALID AS VARCHAR(MAX)) AS avtalid,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS belopp_sek,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS belopp_val,
		CAST(BESTUT_V AS VARCHAR(MAX)) AS bestut_v,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS betalningssparr,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS betalt_sek,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS betalt_val,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS betdagar,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS budget_v,
		CONVERT(varchar(max), DEFDATUM, 126) AS defdatum,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS defsign,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS enhet_id,
		CAST(EREF AS VARCHAR(MAX)) AS eref,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS externanm,
		CAST(EXTERNID AS VARCHAR(MAX)) AS externid,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS externnr,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS faktstatus,
		CONVERT(varchar(max), FAKTURADATUM, 126) AS fakturadatum,
		CONVERT(varchar(max), FAKTURA_REGDATUM, 126) AS faktura_regdatum,
		CAST(FORETAG AS VARCHAR(MAX)) AS foretag,
		CONVERT(varchar(max), FORFALLODATUM, 126) AS forfallodatum,
		CAST(F_DOK_ANTAL AS VARCHAR(MAX)) AS f_dok_antal,
		CAST(F_MED AS VARCHAR(MAX)) AS f_med,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS huvudtext,
		CAST(HÄNDAT_ID AS VARCHAR(MAX)) AS händat_id,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS händ_id,
		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(KATEGORI AS VARCHAR(MAX)) AS kategori,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KRAVNIVA AS VARCHAR(MAX)) AS kravniva,
		CAST(LEVID AS VARCHAR(MAX)) AS levid,
		CAST(LEVRTYP AS VARCHAR(MAX)) AS levrtyp,
		CAST(LPMALL_ID AS VARCHAR(MAX)) AS lpmall_id,
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
		CAST(RAD_ID AS VARCHAR(MAX)) AS rad_id,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS rantedeb,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS regdat_id,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(RESPRO_ID AS VARCHAR(MAX)) AS respro_id,
		CAST(RESPRO_V AS VARCHAR(MAX)) AS respro_v,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS senastbetdatum,
		CAST(SPEC_ID AS VARCHAR(MAX)) AS spec_id,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS tab_betv,
		CAST(TAB_BGCNR AS VARCHAR(MAX)) AS tab_bgcnr,
		CAST(TAB_ENHET AS VARCHAR(MAX)) AS tab_enhet,
		CAST(TAB_KAT AS VARCHAR(MAX)) AS tab_kat,
		CAST(TAB_LPMALL AS VARCHAR(MAX)) AS tab_lpmall,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CAST(TAB_MOMSOM AS VARCHAR(MAX)) AS tab_momsom,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS tab_motp,
		CAST(TAB_ORDNR AS VARCHAR(MAX)) AS tab_ordnr,
		CAST(TAB_SKDAT AS VARCHAR(MAX)) AS tab_skdat,
		CAST(TAB_SKNR AS VARCHAR(MAX)) AS tab_sknr,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS tab_ubf,
		CAST(TAB_UBK AS VARCHAR(MAX)) AS tab_ubk,
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS tab_valuta,
		CAST(TYP_ID AS VARCHAR(MAX)) AS typ_id,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS ursprungs_verifikat,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS ursptext,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTFVAL_V AS VARCHAR(MAX)) AS utfval_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS utskrdatum,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS verdokref,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(VREF AS VARCHAR(MAX)) AS vref,
		CAST(YGRP_ID AS VARCHAR(MAX)) AS ygrp_id 
	FROM ftvudp.ftv_400.RK_FAKTA_LEVFAKT_VERIFIKAT ) y
WHERE _data_modified_utc between DATEADD(month, -3, GETDATE()) and GETDATE()
	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    
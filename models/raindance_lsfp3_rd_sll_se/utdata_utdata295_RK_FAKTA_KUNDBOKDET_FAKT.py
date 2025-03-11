
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ABONNEMANGSID': 'varchar(max)', 'ANM': 'varchar(max)', 'ANSTDATUM': 'varchar(max)', 'ANSTSIGN': 'varchar(max)', 'ANTAL_KORR': 'varchar(max)', 'ATTEST': 'varchar(max)', 'BELOPP_SEK': 'varchar(max)', 'BELOPP_VAL': 'varchar(max)', 'BETALNINGSSPARR': 'varchar(max)', 'BETALT_SEK': 'varchar(max)', 'BETALT_VAL': 'varchar(max)', 'BETDAGAR': 'varchar(max)', 'BETPAMDATUM': 'varchar(max)', 'BETVAG_BANKKONTO': 'varchar(max)', 'BETVAG_BGNR': 'varchar(max)', 'BETVAG_CBNR': 'varchar(max)', 'BETVAG_FACNR': 'varchar(max)', 'BETVAG_PGNR': 'varchar(max)', 'BETVAG_UTLNR': 'varchar(max)', 'BOKBELOPP_INT': 'varchar(max)', 'BOKBELOPP_VAL': 'varchar(max)', 'BOKSTATUS': 'varchar(max)', 'BOKTYP': 'varchar(max)', 'BUNTNR': 'varchar(max)', 'DETALJTYP': 'varchar(max)', 'DOKREF': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'DRANTESATS': 'varchar(max)', 'DUMMY3': 'varchar(max)', 'EREF': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTSTATUS2': 'varchar(max)', 'FAKTURADATUM': 'varchar(max)', 'FORFALLODATUM': 'varchar(max)', 'IRANTEBER_VAL': 'varchar(max)', 'KLARDATUM': 'varchar(max)', 'KRABATTDATUM': 'varchar(max)', 'KRABATT_VAL': 'varchar(max)', 'KRAVNIVA': 'varchar(max)', 'KUNDID': 'varchar(max)', 'KUNDRTYP': 'varchar(max)', 'KURSDIV': 'varchar(max)', 'KURSMULT': 'varchar(max)', 'MED': 'varchar(max)', 'MOMS_SEK': 'varchar(max)', 'MOMS_VAL': 'varchar(max)', 'MOTTATTDAT': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'NR': 'varchar(max)', 'OCRNR': 'varchar(max)', 'ORDERID': 'varchar(max)', 'OVERDRDAGAR': 'varchar(max)', 'RANTEDEB': 'varchar(max)', 'RANTEDEBDATUM': 'varchar(max)', 'RANTEDEB_VAL': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'RESKONTRA': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'TAB_AVTTYP': 'varchar(max)', 'TAB_BEHÄND': 'varchar(max)', 'TAB_BETP': 'varchar(max)', 'TAB_BETV': 'varchar(max)', 'TAB_CMALL': 'varchar(max)', 'TAB_EFAKT': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_RDEB': 'varchar(max)', 'TAB_RESENH': 'varchar(max)', 'UTILITY': 'varchar(max)', 'UTSKRDATUM': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VREF': 'varchar(max)', 'ZVFREFERENS': 'varchar(max)'},
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ABONNEMANGSID AS VARCHAR(MAX)) AS abonnemangsid,
		CAST(ANM AS VARCHAR(MAX)) AS anm,
		CONVERT(varchar(max), ANSTDATUM, 126) AS anstdatum,
		CAST(ANSTSIGN AS VARCHAR(MAX)) AS anstsign,
		CAST(ANTAL_KORR AS VARCHAR(MAX)) AS antal_korr,
		CAST(ATTEST AS VARCHAR(MAX)) AS attest,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS belopp_sek,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS belopp_val,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS betalningssparr,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS betalt_sek,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS betalt_val,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS betdagar,
		CONVERT(varchar(max), BETPAMDATUM, 126) AS betpamdatum,
		CAST(BETVAG_BANKKONTO AS VARCHAR(MAX)) AS betvag_bankkonto,
		CAST(BETVAG_BGNR AS VARCHAR(MAX)) AS betvag_bgnr,
		CAST(BETVAG_CBNR AS VARCHAR(MAX)) AS betvag_cbnr,
		CAST(BETVAG_FACNR AS VARCHAR(MAX)) AS betvag_facnr,
		CAST(BETVAG_PGNR AS VARCHAR(MAX)) AS betvag_pgnr,
		CAST(BETVAG_UTLNR AS VARCHAR(MAX)) AS betvag_utlnr,
		CAST(BOKBELOPP_INT AS VARCHAR(MAX)) AS bokbelopp_int,
		CAST(BOKBELOPP_VAL AS VARCHAR(MAX)) AS bokbelopp_val,
		CAST(BOKSTATUS AS VARCHAR(MAX)) AS bokstatus,
		CAST(BOKTYP AS VARCHAR(MAX)) AS boktyp,
		CAST(BUNTNR AS VARCHAR(MAX)) AS buntnr,
		CAST(DETALJTYP AS VARCHAR(MAX)) AS detaljtyp,
		CAST(DOKREF AS VARCHAR(MAX)) AS dokref,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS dok_antal,
		CAST(DRANTESATS AS VARCHAR(MAX)) AS drantesats,
		CAST(DUMMY3 AS VARCHAR(MAX)) AS dummy3,
		CAST(EREF AS VARCHAR(MAX)) AS eref,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS faktstatus,
		CAST(FAKTSTATUS2 AS VARCHAR(MAX)) AS faktstatus2,
		CONVERT(varchar(max), FAKTURADATUM, 126) AS fakturadatum,
		CONVERT(varchar(max), FORFALLODATUM, 126) AS forfallodatum,
		CAST(IRANTEBER_VAL AS VARCHAR(MAX)) AS iranteber_val,
		CONVERT(varchar(max), KLARDATUM, 126) AS klardatum,
		CONVERT(varchar(max), KRABATTDATUM, 126) AS krabattdatum,
		CAST(KRABATT_VAL AS VARCHAR(MAX)) AS krabatt_val,
		CAST(KRAVNIVA AS VARCHAR(MAX)) AS kravniva,
		CAST(KUNDID AS VARCHAR(MAX)) AS kundid,
		CAST(KUNDRTYP AS VARCHAR(MAX)) AS kundrtyp,
		CAST(KURSDIV AS VARCHAR(MAX)) AS kursdiv,
		CAST(KURSMULT AS VARCHAR(MAX)) AS kursmult,
		CAST(MED AS VARCHAR(MAX)) AS med,
		CAST(MOMS_SEK AS VARCHAR(MAX)) AS moms_sek,
		CAST(MOMS_VAL AS VARCHAR(MAX)) AS moms_val,
		CONVERT(varchar(max), MOTTATTDAT, 126) AS mottattdat,
		CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS mottattsign,
		CAST(NR AS VARCHAR(MAX)) AS nr,
		CAST(OCRNR AS VARCHAR(MAX)) AS ocrnr,
		CAST(ORDERID AS VARCHAR(MAX)) AS orderid,
		CAST(OVERDRDAGAR AS VARCHAR(MAX)) AS overdrdagar,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS rantedeb,
		CONVERT(varchar(max), RANTEDEBDATUM, 126) AS rantedebdatum,
		CAST(RANTEDEB_VAL AS VARCHAR(MAX)) AS rantedeb_val,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(RESKONTRA AS VARCHAR(MAX)) AS reskontra,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS senastbetdatum,
		CAST(TAB_AVTTYP AS VARCHAR(MAX)) AS tab_avttyp,
		CAST(TAB_BEHÄND AS VARCHAR(MAX)) AS tab_behänd,
		CAST(TAB_BETP AS VARCHAR(MAX)) AS tab_betp,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS tab_betv,
		CAST(TAB_CMALL AS VARCHAR(MAX)) AS tab_cmall,
		CAST(TAB_EFAKT AS VARCHAR(MAX)) AS tab_efakt,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS tab_motp,
		CAST(TAB_RDEB AS VARCHAR(MAX)) AS tab_rdeb,
		CAST(TAB_RESENH AS VARCHAR(MAX)) AS tab_resenh,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS utskrdatum,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad,
		CAST(VREF AS VARCHAR(MAX)) AS vref,
		CAST(ZVFREFERENS AS VARCHAR(MAX)) AS zvfreferens 
	FROM utdata.utdata295.RK_FAKTA_KUNDBOKDET_FAKT) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
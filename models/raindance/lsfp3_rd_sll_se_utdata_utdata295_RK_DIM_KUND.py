
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ADR1': 'varchar(40)',
 'ADR2': 'varchar(40)',
 'ATTRIBUTE_ACTORID': 'varchar(40)',
 'ATTRIBUTE_EAN': 'varchar(13)',
 'BETSPARR': 'bit',
 'BGNR': 'varchar(16)',
 'DUMMY1': 'varchar(16)',
 'ELEKTRONISK_KOMMUNIKATION': 'varchar(1)',
 'ENGANG': 'bit',
 'FRI1': 'varchar(40)',
 'FRI2': 'varchar(40)',
 'FRI3': 'varchar(40)',
 'FRI4': 'varchar(40)',
 'GRUPP': 'varchar(20)',
 'KREDITLIMIT': 'numeric',
 'KUNDID': 'varchar(16)',
 'KUNDID2': 'varchar(16)',
 'KUNDID_ID_TEXT': 'varchar(57)',
 'KUNDID_TEXT': 'varchar(40)',
 'KUND_EREF': 'varchar(40)',
 'KUND_TAB_AVTTYP': 'varchar(4)',
 'KUND_TAB_BEHÄND': 'varchar(4)',
 'KUND_TAB_BETP': 'varchar(2)',
 'KUND_TAB_BETV': 'varchar(2)',
 'KUND_TAB_CMALL': 'varchar(14)',
 'KUND_TAB_EFAKT': 'varchar(1)',
 'KUND_TAB_MOMS': 'varchar(2)',
 'KUND_TAB_MOTP': 'varchar(4)',
 'KUND_TAB_RDEB': 'varchar(2)',
 'KUND_TAB_RESENH': 'varchar(3)',
 'KUND_TAB_SPRÅK': 'varchar(1)',
 'KUND_VREF': 'varchar(40)',
 'LAND': 'varchar(2)',
 'MOMSREG': 'varchar(40)',
 'MSG_INVOIC_FORMAT': 'varchar(15)',
 'MSG_INVOIC_PA': 'varchar(1)',
 'MSG_INVOIC_TRANSPORT': 'varchar(75)',
 'MSG_INVOIC_VAG': 'varchar(3)',
 'NAMN2': 'varchar(40)',
 'OMSATTNING': 'numeric',
 'OMSATTNINGSAR': 'datetime',
 'OMSATTNING_FG': 'numeric',
 'ORGNR': 'varchar(40)',
 'ORT': 'varchar(40)',
 'PASSIV': 'bit',
 'PGNR': 'varchar(16)',
 'SALDO': 'numeric',
 'SOKBEGR': 'varchar(40)',
 'TELNR': 'varchar(40)'},
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
 		CAST(ADR1 AS VARCHAR(MAX)) AS adr1,
		CAST(ADR2 AS VARCHAR(MAX)) AS adr2,
		CAST(ATTRIBUTE_ACTORID AS VARCHAR(MAX)) AS attribute_actorid,
		CAST(ATTRIBUTE_EAN AS VARCHAR(MAX)) AS attribute_ean,
		CAST(BETSPARR AS VARCHAR(MAX)) AS betsparr,
		CAST(BGNR AS VARCHAR(MAX)) AS bgnr,
		CAST(DUMMY1 AS VARCHAR(MAX)) AS dummy1,
		CAST(ELEKTRONISK_KOMMUNIKATION AS VARCHAR(MAX)) AS elektronisk_kommunikation,
		CAST(ENGANG AS VARCHAR(MAX)) AS engang,
		CAST(FRI1 AS VARCHAR(MAX)) AS fri1,
		CAST(FRI2 AS VARCHAR(MAX)) AS fri2,
		CAST(FRI3 AS VARCHAR(MAX)) AS fri3,
		CAST(FRI4 AS VARCHAR(MAX)) AS fri4,
		CAST(GRUPP AS VARCHAR(MAX)) AS grupp,
		CAST(KREDITLIMIT AS VARCHAR(MAX)) AS kreditlimit,
		CAST(KUND_EREF AS VARCHAR(MAX)) AS kund_eref,
		CAST(KUND_TAB_AVTTYP AS VARCHAR(MAX)) AS kund_tab_avttyp,
		CAST(KUND_TAB_BEHÄND AS VARCHAR(MAX)) AS kund_tab_behänd,
		CAST(KUND_TAB_BETP AS VARCHAR(MAX)) AS kund_tab_betp,
		CAST(KUND_TAB_BETV AS VARCHAR(MAX)) AS kund_tab_betv,
		CAST(KUND_TAB_CMALL AS VARCHAR(MAX)) AS kund_tab_cmall,
		CAST(KUND_TAB_EFAKT AS VARCHAR(MAX)) AS kund_tab_efakt,
		CAST(KUND_TAB_MOMS AS VARCHAR(MAX)) AS kund_tab_moms,
		CAST(KUND_TAB_MOTP AS VARCHAR(MAX)) AS kund_tab_motp,
		CAST(KUND_TAB_RDEB AS VARCHAR(MAX)) AS kund_tab_rdeb,
		CAST(KUND_TAB_RESENH AS VARCHAR(MAX)) AS kund_tab_resenh,
		CAST(KUND_TAB_SPRÅK AS VARCHAR(MAX)) AS kund_tab_språk,
		CAST(KUND_VREF AS VARCHAR(MAX)) AS kund_vref,
		CAST(KUNDID AS VARCHAR(MAX)) AS kundid,
		CAST(KUNDID_ID_TEXT AS VARCHAR(MAX)) AS kundid_id_text,
		CAST(KUNDID_TEXT AS VARCHAR(MAX)) AS kundid_text,
		CAST(KUNDID2 AS VARCHAR(MAX)) AS kundid2,
		CAST(LAND AS VARCHAR(MAX)) AS land,
		CAST(MOMSREG AS VARCHAR(MAX)) AS momsreg,
		CAST(MSG_INVOIC_FORMAT AS VARCHAR(MAX)) AS msg_invoic_format,
		CAST(MSG_INVOIC_PA AS VARCHAR(MAX)) AS msg_invoic_pa,
		CAST(MSG_INVOIC_TRANSPORT AS VARCHAR(MAX)) AS msg_invoic_transport,
		CAST(MSG_INVOIC_VAG AS VARCHAR(MAX)) AS msg_invoic_vag,
		CAST(NAMN2 AS VARCHAR(MAX)) AS namn2,
		CAST(OMSATTNING AS VARCHAR(MAX)) AS omsattning,
		CAST(OMSATTNING_FG AS VARCHAR(MAX)) AS omsattning_fg,
		CONVERT(varchar(max), OMSATTNINGSAR, 126) AS omsattningsar,
		CAST(ORGNR AS VARCHAR(MAX)) AS orgnr,
		CAST(ORT AS VARCHAR(MAX)) AS ort,
		CAST(PASSIV AS VARCHAR(MAX)) AS passiv,
		CAST(PGNR AS VARCHAR(MAX)) AS pgnr,
		CAST(SALDO AS VARCHAR(MAX)) AS saldo,
		CAST(SOKBEGR AS VARCHAR(MAX)) AS sokbegr,
		CAST(TELNR AS VARCHAR(MAX)) AS telnr 
	FROM utdata.utdata295.RK_DIM_KUND
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")

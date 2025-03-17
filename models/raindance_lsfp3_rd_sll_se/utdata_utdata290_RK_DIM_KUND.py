
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ADR1': 'varchar(max)', 'ADR2': 'varchar(max)', 'ATTRIBUTE_ACTORID': 'varchar(max)', 'ATTRIBUTE_EAN': 'varchar(max)', 'BETSPARR': 'varchar(max)', 'BGNR': 'varchar(max)', 'DUMMY1': 'varchar(max)', 'ELEKTRONISK_KOMMUNIKATION': 'varchar(max)', 'ENGANG': 'varchar(max)', 'FRI1': 'varchar(max)', 'FRI2': 'varchar(max)', 'FRI3': 'varchar(max)', 'FRI4': 'varchar(max)', 'GRUPP': 'varchar(max)', 'KREDITLIMIT': 'varchar(max)', 'KUNDID': 'varchar(max)', 'KUNDID2': 'varchar(max)', 'KUNDID_ID_TEXT': 'varchar(max)', 'KUNDID_TEXT': 'varchar(max)', 'KUND_EREF': 'varchar(max)', 'KUND_TAB_BEHÄND': 'varchar(max)', 'KUND_TAB_BETP': 'varchar(max)', 'KUND_TAB_BETV': 'varchar(max)', 'KUND_TAB_MOMS': 'varchar(max)', 'KUND_TAB_MOTP': 'varchar(max)', 'KUND_TAB_RDEB': 'varchar(max)', 'KUND_TAB_SPRÅK': 'varchar(max)', 'KUND_VREF': 'varchar(max)', 'LAND': 'varchar(max)', 'MOMSREG': 'varchar(max)', 'MSG_INVOIC_FORMAT': 'varchar(max)', 'MSG_INVOIC_PA': 'varchar(max)', 'MSG_INVOIC_TRANSPORT': 'varchar(max)', 'MSG_INVOIC_VAG': 'varchar(max)', 'NAMN2': 'varchar(max)', 'OMSATTNING': 'varchar(max)', 'OMSATTNINGSAR': 'varchar(max)', 'OMSATTNING_FG': 'varchar(max)', 'ORGNR': 'varchar(max)', 'ORT': 'varchar(max)', 'PASSIV': 'varchar(max)', 'PGNR': 'varchar(max)', 'SALDO': 'varchar(max)', 'SOKBEGR': 'varchar(max)', 'TELNR': 'varchar(max)'},
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
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata290' as _source,
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
		CAST(KUNDID AS VARCHAR(MAX)) AS kundid,
		CAST(KUNDID2 AS VARCHAR(MAX)) AS kundid2,
		CAST(KUNDID_ID_TEXT AS VARCHAR(MAX)) AS kundid_id_text,
		CAST(KUNDID_TEXT AS VARCHAR(MAX)) AS kundid_text,
		CAST(KUND_EREF AS VARCHAR(MAX)) AS kund_eref,
		CAST(KUND_TAB_BEHÄND AS VARCHAR(MAX)) AS kund_tab_behänd,
		CAST(KUND_TAB_BETP AS VARCHAR(MAX)) AS kund_tab_betp,
		CAST(KUND_TAB_BETV AS VARCHAR(MAX)) AS kund_tab_betv,
		CAST(KUND_TAB_MOMS AS VARCHAR(MAX)) AS kund_tab_moms,
		CAST(KUND_TAB_MOTP AS VARCHAR(MAX)) AS kund_tab_motp,
		CAST(KUND_TAB_RDEB AS VARCHAR(MAX)) AS kund_tab_rdeb,
		CAST(KUND_TAB_SPRÅK AS VARCHAR(MAX)) AS kund_tab_språk,
		CAST(KUND_VREF AS VARCHAR(MAX)) AS kund_vref,
		CAST(LAND AS VARCHAR(MAX)) AS land,
		CAST(MOMSREG AS VARCHAR(MAX)) AS momsreg,
		CAST(MSG_INVOIC_FORMAT AS VARCHAR(MAX)) AS msg_invoic_format,
		CAST(MSG_INVOIC_PA AS VARCHAR(MAX)) AS msg_invoic_pa,
		CAST(MSG_INVOIC_TRANSPORT AS VARCHAR(MAX)) AS msg_invoic_transport,
		CAST(MSG_INVOIC_VAG AS VARCHAR(MAX)) AS msg_invoic_vag,
		CAST(NAMN2 AS VARCHAR(MAX)) AS namn2,
		CAST(OMSATTNING AS VARCHAR(MAX)) AS omsattning,
		CONVERT(varchar(max), OMSATTNINGSAR, 126) AS omsattningsar,
		CAST(OMSATTNING_FG AS VARCHAR(MAX)) AS omsattning_fg,
		CAST(ORGNR AS VARCHAR(MAX)) AS orgnr,
		CAST(ORT AS VARCHAR(MAX)) AS ort,
		CAST(PASSIV AS VARCHAR(MAX)) AS passiv,
		CAST(PGNR AS VARCHAR(MAX)) AS pgnr,
		CAST(SALDO AS VARCHAR(MAX)) AS saldo,
		CAST(SOKBEGR AS VARCHAR(MAX)) AS sokbegr,
		CAST(TELNR AS VARCHAR(MAX)) AS telnr 
	FROM utdata.utdata290.RK_DIM_KUND ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
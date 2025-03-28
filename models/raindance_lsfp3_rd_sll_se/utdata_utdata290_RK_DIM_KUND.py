
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
		CAST(ADR1 AS VARCHAR(MAX)) AS ADR1,
		CAST(ADR2 AS VARCHAR(MAX)) AS ADR2,
		CAST(ATTRIBUTE_ACTORID AS VARCHAR(MAX)) AS ATTRIBUTE_ACTORID,
		CAST(ATTRIBUTE_EAN AS VARCHAR(MAX)) AS ATTRIBUTE_EAN,
		CAST(BETSPARR AS VARCHAR(MAX)) AS BETSPARR,
		CAST(BGNR AS VARCHAR(MAX)) AS BGNR,
		CAST(DUMMY1 AS VARCHAR(MAX)) AS DUMMY1,
		CAST(ELEKTRONISK_KOMMUNIKATION AS VARCHAR(MAX)) AS ELEKTRONISK_KOMMUNIKATION,
		CAST(ENGANG AS VARCHAR(MAX)) AS ENGANG,
		CAST(FRI1 AS VARCHAR(MAX)) AS FRI1,
		CAST(FRI2 AS VARCHAR(MAX)) AS FRI2,
		CAST(FRI3 AS VARCHAR(MAX)) AS FRI3,
		CAST(FRI4 AS VARCHAR(MAX)) AS FRI4,
		CAST(GRUPP AS VARCHAR(MAX)) AS GRUPP,
		CAST(KREDITLIMIT AS VARCHAR(MAX)) AS KREDITLIMIT,
		CAST(KUNDID AS VARCHAR(MAX)) AS KUNDID,
		CAST(KUNDID2 AS VARCHAR(MAX)) AS KUNDID2,
		CAST(KUNDID_ID_TEXT AS VARCHAR(MAX)) AS KUNDID_ID_TEXT,
		CAST(KUNDID_TEXT AS VARCHAR(MAX)) AS KUNDID_TEXT,
		CAST(KUND_EREF AS VARCHAR(MAX)) AS KUND_EREF,
		CAST(KUND_TAB_BEHÄND AS VARCHAR(MAX)) AS KUND_TAB_BEHÄND,
		CAST(KUND_TAB_BETP AS VARCHAR(MAX)) AS KUND_TAB_BETP,
		CAST(KUND_TAB_BETV AS VARCHAR(MAX)) AS KUND_TAB_BETV,
		CAST(KUND_TAB_MOMS AS VARCHAR(MAX)) AS KUND_TAB_MOMS,
		CAST(KUND_TAB_MOTP AS VARCHAR(MAX)) AS KUND_TAB_MOTP,
		CAST(KUND_TAB_RDEB AS VARCHAR(MAX)) AS KUND_TAB_RDEB,
		CAST(KUND_TAB_SPRÅK AS VARCHAR(MAX)) AS KUND_TAB_SPRÅK,
		CAST(KUND_VREF AS VARCHAR(MAX)) AS KUND_VREF,
		CAST(LAND AS VARCHAR(MAX)) AS LAND,
		CAST(MOMSREG AS VARCHAR(MAX)) AS MOMSREG,
		CAST(MSG_INVOIC_FORMAT AS VARCHAR(MAX)) AS MSG_INVOIC_FORMAT,
		CAST(MSG_INVOIC_PA AS VARCHAR(MAX)) AS MSG_INVOIC_PA,
		CAST(MSG_INVOIC_TRANSPORT AS VARCHAR(MAX)) AS MSG_INVOIC_TRANSPORT,
		CAST(MSG_INVOIC_VAG AS VARCHAR(MAX)) AS MSG_INVOIC_VAG,
		CAST(NAMN2 AS VARCHAR(MAX)) AS NAMN2,
		CAST(OMSATTNING AS VARCHAR(MAX)) AS OMSATTNING,
		CONVERT(varchar(max), OMSATTNINGSAR, 126) AS OMSATTNINGSAR,
		CAST(OMSATTNING_FG AS VARCHAR(MAX)) AS OMSATTNING_FG,
		CAST(ORGNR AS VARCHAR(MAX)) AS ORGNR,
		CAST(ORT AS VARCHAR(MAX)) AS ORT,
		CAST(PASSIV AS VARCHAR(MAX)) AS PASSIV,
		CAST(PGNR AS VARCHAR(MAX)) AS PGNR,
		CAST(SALDO AS VARCHAR(MAX)) AS SALDO,
		CAST(SOKBEGR AS VARCHAR(MAX)) AS SOKBEGR,
		CAST(TELNR AS VARCHAR(MAX)) AS TELNR 
	FROM utdata.utdata290.RK_DIM_KUND ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
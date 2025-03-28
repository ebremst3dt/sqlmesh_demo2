
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ADR1': 'varchar(max)', 'ADR2': 'varchar(max)', 'ATTRIBUTE_ACTORID': 'varchar(max)', 'ATTRIBUTE_EAN': 'varchar(max)', 'BANKADR': 'varchar(max)', 'BANKKONTO': 'varchar(max)', 'BANKLAND': 'varchar(max)', 'BANKNAMN': 'varchar(max)', 'BANKORT': 'varchar(max)', 'BBAN': 'varchar(max)', 'BETALVAG': 'varchar(max)', 'BETSPARR': 'varchar(max)', 'BGNR': 'varchar(max)', 'BIC': 'varchar(max)', 'DUMMY1': 'varchar(max)', 'ELEKTRONISK_KOMMUNIKATION': 'varchar(max)', 'ENGANG': 'varchar(max)', 'FACBG': 'varchar(max)', 'FACNAMN': 'varchar(max)', 'FACPG': 'varchar(max)', 'FRI1': 'varchar(max)', 'FRI2': 'varchar(max)', 'FRI3': 'varchar(max)', 'FRI4': 'varchar(max)', 'GRUPP': 'varchar(max)', 'IBAN': 'varchar(max)', 'KREDITLIMIT': 'varchar(max)', 'LAND': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVID2': 'varchar(max)', 'LEVID_ID_TEXT': 'varchar(max)', 'LEVID_TEXT': 'varchar(max)', 'LEV_EREF': 'varchar(max)', 'LEV_TAB_BETV': 'varchar(max)', 'LEV_TAB_CMALL': 'varchar(max)', 'LEV_TAB_FÖRK': 'varchar(max)', 'LEV_TAB_MOMS': 'varchar(max)', 'LEV_TAB_MOTP': 'varchar(max)', 'LEV_TAB_REF': 'varchar(max)', 'LEV_TAB_RESK': 'varchar(max)', 'LEV_TAB_SCADAT': 'varchar(max)', 'LEV_TAB_SCANNR': 'varchar(max)', 'LEV_TAB_UBF': 'varchar(max)', 'LEV_TAB_UBK': 'varchar(max)', 'LEV_TAB_VALUTA': 'varchar(max)', 'LEV_VREF': 'varchar(max)', 'MOMSREG': 'varchar(max)', 'MSG_DESADV_FORMAT': 'varchar(max)', 'MSG_DESADV_PA': 'varchar(max)', 'MSG_DESADV_TRANSPORT': 'varchar(max)', 'MSG_DESADV_VAG': 'varchar(max)', 'MSG_EXTORD_FORMAT': 'varchar(max)', 'MSG_EXTORD_PA': 'varchar(max)', 'MSG_EXTORD_TRANSPORT': 'varchar(max)', 'MSG_EXTORD_VAG': 'varchar(max)', 'MSG_INVOIC_FORMAT': 'varchar(max)', 'MSG_INVOIC_PA': 'varchar(max)', 'MSG_INVOIC_TRANSPORT': 'varchar(max)', 'MSG_INVOIC_VAG': 'varchar(max)', 'MSG_MSCONS_FORMAT': 'varchar(max)', 'MSG_MSCONS_PA': 'varchar(max)', 'MSG_MSCONS_TRANSPORT': 'varchar(max)', 'MSG_MSCONS_VAG': 'varchar(max)', 'MSG_ORDERS_FORMAT': 'varchar(max)', 'MSG_ORDERS_PA': 'varchar(max)', 'MSG_ORDERS_TRANSPORT': 'varchar(max)', 'MSG_ORDERS_VAG': 'varchar(max)', 'MSG_ORDRSP_FORMAT': 'varchar(max)', 'MSG_ORDRSP_PA': 'varchar(max)', 'MSG_ORDRSP_TRANSPORT': 'varchar(max)', 'MSG_ORDRSP_VAG': 'varchar(max)', 'MSG_PRICAT_FORMAT': 'varchar(max)', 'MSG_PRICAT_PA': 'varchar(max)', 'MSG_PRICAT_TRANSPORT': 'varchar(max)', 'MSG_PRICAT_VAG': 'varchar(max)', 'MSG_SHOPCART_FORMAT': 'varchar(max)', 'MSG_SHOPCART_PA': 'varchar(max)', 'MSG_SHOPCART_TRANSPORT': 'varchar(max)', 'MSG_SHOPCART_VAG': 'varchar(max)', 'NAMN2': 'varchar(max)', 'NATCLEARKOD': 'varchar(max)', 'OMSATTNING': 'varchar(max)', 'OMSATTNINGSAR': 'varchar(max)', 'OMSATTNING_FG': 'varchar(max)', 'ORGNR': 'varchar(max)', 'ORT': 'varchar(max)', 'PASSIV': 'varchar(max)', 'PGNR': 'varchar(max)', 'SALDO': 'varchar(max)', 'SEPA': 'varchar(max)', 'SOKBEGR': 'varchar(max)', 'SWIFTADR': 'varchar(max)', 'TELNR': 'varchar(max)', 'TERMIN': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata287' as _source,
		CAST(ADR1 AS VARCHAR(MAX)) AS ADR1,
		CAST(ADR2 AS VARCHAR(MAX)) AS ADR2,
		CAST(ATTRIBUTE_ACTORID AS VARCHAR(MAX)) AS ATTRIBUTE_ACTORID,
		CAST(ATTRIBUTE_EAN AS VARCHAR(MAX)) AS ATTRIBUTE_EAN,
		CAST(BANKADR AS VARCHAR(MAX)) AS BANKADR,
		CAST(BANKKONTO AS VARCHAR(MAX)) AS BANKKONTO,
		CAST(BANKLAND AS VARCHAR(MAX)) AS BANKLAND,
		CAST(BANKNAMN AS VARCHAR(MAX)) AS BANKNAMN,
		CAST(BANKORT AS VARCHAR(MAX)) AS BANKORT,
		CAST(BBAN AS VARCHAR(MAX)) AS BBAN,
		CAST(BETALVAG AS VARCHAR(MAX)) AS BETALVAG,
		CAST(BETSPARR AS VARCHAR(MAX)) AS BETSPARR,
		CAST(BGNR AS VARCHAR(MAX)) AS BGNR,
		CAST(BIC AS VARCHAR(MAX)) AS BIC,
		CAST(DUMMY1 AS VARCHAR(MAX)) AS DUMMY1,
		CAST(ELEKTRONISK_KOMMUNIKATION AS VARCHAR(MAX)) AS ELEKTRONISK_KOMMUNIKATION,
		CAST(ENGANG AS VARCHAR(MAX)) AS ENGANG,
		CAST(FACBG AS VARCHAR(MAX)) AS FACBG,
		CAST(FACNAMN AS VARCHAR(MAX)) AS FACNAMN,
		CAST(FACPG AS VARCHAR(MAX)) AS FACPG,
		CAST(FRI1 AS VARCHAR(MAX)) AS FRI1,
		CAST(FRI2 AS VARCHAR(MAX)) AS FRI2,
		CAST(FRI3 AS VARCHAR(MAX)) AS FRI3,
		CAST(FRI4 AS VARCHAR(MAX)) AS FRI4,
		CAST(GRUPP AS VARCHAR(MAX)) AS GRUPP,
		CAST(IBAN AS VARCHAR(MAX)) AS IBAN,
		CAST(KREDITLIMIT AS VARCHAR(MAX)) AS KREDITLIMIT,
		CAST(LAND AS VARCHAR(MAX)) AS LAND,
		CAST(LEVID AS VARCHAR(MAX)) AS LEVID,
		CAST(LEVID2 AS VARCHAR(MAX)) AS LEVID2,
		CAST(LEVID_ID_TEXT AS VARCHAR(MAX)) AS LEVID_ID_TEXT,
		CAST(LEVID_TEXT AS VARCHAR(MAX)) AS LEVID_TEXT,
		CAST(LEV_EREF AS VARCHAR(MAX)) AS LEV_EREF,
		CAST(LEV_TAB_BETV AS VARCHAR(MAX)) AS LEV_TAB_BETV,
		CAST(LEV_TAB_CMALL AS VARCHAR(MAX)) AS LEV_TAB_CMALL,
		CAST(LEV_TAB_FÖRK AS VARCHAR(MAX)) AS LEV_TAB_FÖRK,
		CAST(LEV_TAB_MOMS AS VARCHAR(MAX)) AS LEV_TAB_MOMS,
		CAST(LEV_TAB_MOTP AS VARCHAR(MAX)) AS LEV_TAB_MOTP,
		CAST(LEV_TAB_REF AS VARCHAR(MAX)) AS LEV_TAB_REF,
		CAST(LEV_TAB_RESK AS VARCHAR(MAX)) AS LEV_TAB_RESK,
		CAST(LEV_TAB_SCADAT AS VARCHAR(MAX)) AS LEV_TAB_SCADAT,
		CAST(LEV_TAB_SCANNR AS VARCHAR(MAX)) AS LEV_TAB_SCANNR,
		CAST(LEV_TAB_UBF AS VARCHAR(MAX)) AS LEV_TAB_UBF,
		CAST(LEV_TAB_UBK AS VARCHAR(MAX)) AS LEV_TAB_UBK,
		CAST(LEV_TAB_VALUTA AS VARCHAR(MAX)) AS LEV_TAB_VALUTA,
		CAST(LEV_VREF AS VARCHAR(MAX)) AS LEV_VREF,
		CAST(MOMSREG AS VARCHAR(MAX)) AS MOMSREG,
		CAST(MSG_DESADV_FORMAT AS VARCHAR(MAX)) AS MSG_DESADV_FORMAT,
		CAST(MSG_DESADV_PA AS VARCHAR(MAX)) AS MSG_DESADV_PA,
		CAST(MSG_DESADV_TRANSPORT AS VARCHAR(MAX)) AS MSG_DESADV_TRANSPORT,
		CAST(MSG_DESADV_VAG AS VARCHAR(MAX)) AS MSG_DESADV_VAG,
		CAST(MSG_EXTORD_FORMAT AS VARCHAR(MAX)) AS MSG_EXTORD_FORMAT,
		CAST(MSG_EXTORD_PA AS VARCHAR(MAX)) AS MSG_EXTORD_PA,
		CAST(MSG_EXTORD_TRANSPORT AS VARCHAR(MAX)) AS MSG_EXTORD_TRANSPORT,
		CAST(MSG_EXTORD_VAG AS VARCHAR(MAX)) AS MSG_EXTORD_VAG,
		CAST(MSG_INVOIC_FORMAT AS VARCHAR(MAX)) AS MSG_INVOIC_FORMAT,
		CAST(MSG_INVOIC_PA AS VARCHAR(MAX)) AS MSG_INVOIC_PA,
		CAST(MSG_INVOIC_TRANSPORT AS VARCHAR(MAX)) AS MSG_INVOIC_TRANSPORT,
		CAST(MSG_INVOIC_VAG AS VARCHAR(MAX)) AS MSG_INVOIC_VAG,
		CAST(MSG_MSCONS_FORMAT AS VARCHAR(MAX)) AS MSG_MSCONS_FORMAT,
		CAST(MSG_MSCONS_PA AS VARCHAR(MAX)) AS MSG_MSCONS_PA,
		CAST(MSG_MSCONS_TRANSPORT AS VARCHAR(MAX)) AS MSG_MSCONS_TRANSPORT,
		CAST(MSG_MSCONS_VAG AS VARCHAR(MAX)) AS MSG_MSCONS_VAG,
		CAST(MSG_ORDERS_FORMAT AS VARCHAR(MAX)) AS MSG_ORDERS_FORMAT,
		CAST(MSG_ORDERS_PA AS VARCHAR(MAX)) AS MSG_ORDERS_PA,
		CAST(MSG_ORDERS_TRANSPORT AS VARCHAR(MAX)) AS MSG_ORDERS_TRANSPORT,
		CAST(MSG_ORDERS_VAG AS VARCHAR(MAX)) AS MSG_ORDERS_VAG,
		CAST(MSG_ORDRSP_FORMAT AS VARCHAR(MAX)) AS MSG_ORDRSP_FORMAT,
		CAST(MSG_ORDRSP_PA AS VARCHAR(MAX)) AS MSG_ORDRSP_PA,
		CAST(MSG_ORDRSP_TRANSPORT AS VARCHAR(MAX)) AS MSG_ORDRSP_TRANSPORT,
		CAST(MSG_ORDRSP_VAG AS VARCHAR(MAX)) AS MSG_ORDRSP_VAG,
		CAST(MSG_PRICAT_FORMAT AS VARCHAR(MAX)) AS MSG_PRICAT_FORMAT,
		CAST(MSG_PRICAT_PA AS VARCHAR(MAX)) AS MSG_PRICAT_PA,
		CAST(MSG_PRICAT_TRANSPORT AS VARCHAR(MAX)) AS MSG_PRICAT_TRANSPORT,
		CAST(MSG_PRICAT_VAG AS VARCHAR(MAX)) AS MSG_PRICAT_VAG,
		CAST(MSG_SHOPCART_FORMAT AS VARCHAR(MAX)) AS MSG_SHOPCART_FORMAT,
		CAST(MSG_SHOPCART_PA AS VARCHAR(MAX)) AS MSG_SHOPCART_PA,
		CAST(MSG_SHOPCART_TRANSPORT AS VARCHAR(MAX)) AS MSG_SHOPCART_TRANSPORT,
		CAST(MSG_SHOPCART_VAG AS VARCHAR(MAX)) AS MSG_SHOPCART_VAG,
		CAST(NAMN2 AS VARCHAR(MAX)) AS NAMN2,
		CAST(NATCLEARKOD AS VARCHAR(MAX)) AS NATCLEARKOD,
		CAST(OMSATTNING AS VARCHAR(MAX)) AS OMSATTNING,
		CONVERT(varchar(max), OMSATTNINGSAR, 126) AS OMSATTNINGSAR,
		CAST(OMSATTNING_FG AS VARCHAR(MAX)) AS OMSATTNING_FG,
		CAST(ORGNR AS VARCHAR(MAX)) AS ORGNR,
		CAST(ORT AS VARCHAR(MAX)) AS ORT,
		CAST(PASSIV AS VARCHAR(MAX)) AS PASSIV,
		CAST(PGNR AS VARCHAR(MAX)) AS PGNR,
		CAST(SALDO AS VARCHAR(MAX)) AS SALDO,
		CAST(SEPA AS VARCHAR(MAX)) AS SEPA,
		CAST(SOKBEGR AS VARCHAR(MAX)) AS SOKBEGR,
		CAST(SWIFTADR AS VARCHAR(MAX)) AS SWIFTADR,
		CAST(TELNR AS VARCHAR(MAX)) AS TELNR,
		CAST(TERMIN AS VARCHAR(MAX)) AS TERMIN 
	FROM utdata.utdata287.RK_DIM_LEV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
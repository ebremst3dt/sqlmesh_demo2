
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ADR1': 'varchar(max)', 'ADR2': 'varchar(max)', 'ATTRIBUTE_ACTORID': 'varchar(max)', 'ATTRIBUTE_EAN': 'varchar(max)', 'BANKADR': 'varchar(max)', 'BANKKONTO': 'varchar(max)', 'BANKLAND': 'varchar(max)', 'BANKNAMN': 'varchar(max)', 'BANKORT': 'varchar(max)', 'BBAN': 'varchar(max)', 'BETALVAG': 'varchar(max)', 'BETSPARR': 'varchar(max)', 'BGNR': 'varchar(max)', 'BIC': 'varchar(max)', 'DUMMY1': 'varchar(max)', 'ELEKTRONISK_KOMMUNIKATION': 'varchar(max)', 'ENGANG': 'varchar(max)', 'FACBG': 'varchar(max)', 'FACNAMN': 'varchar(max)', 'FACPG': 'varchar(max)', 'FRI1': 'varchar(max)', 'FRI2': 'varchar(max)', 'FRI3': 'varchar(max)', 'FRI4': 'varchar(max)', 'GRUPP': 'varchar(max)', 'IBAN': 'varchar(max)', 'KREDITLIMIT': 'varchar(max)', 'LAND': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVID2': 'varchar(max)', 'LEVID_ID_TEXT': 'varchar(max)', 'LEVID_TEXT': 'varchar(max)', 'LEV_EREF': 'varchar(max)', 'LEV_TAB_AVTRAM': 'varchar(max)', 'LEV_TAB_BETV': 'varchar(max)', 'LEV_TAB_BEVAK': 'varchar(max)', 'LEV_TAB_CMALL': 'varchar(max)', 'LEV_TAB_FÖRK': 'varchar(max)', 'LEV_TAB_MOMS': 'varchar(max)', 'LEV_TAB_MOTP': 'varchar(max)', 'LEV_TAB_REF': 'varchar(max)', 'LEV_TAB_RESENH': 'varchar(max)', 'LEV_TAB_RESK': 'varchar(max)', 'LEV_TAB_SCADAT': 'varchar(max)', 'LEV_TAB_SCANNR': 'varchar(max)', 'LEV_TAB_UBF': 'varchar(max)', 'LEV_TAB_UBK': 'varchar(max)', 'LEV_TAB_VALUTA': 'varchar(max)', 'LEV_VREF': 'varchar(max)', 'MOMSREG': 'varchar(max)', 'MSG_DESADV_FORMAT': 'varchar(max)', 'MSG_DESADV_PA': 'varchar(max)', 'MSG_DESADV_TRANSPORT': 'varchar(max)', 'MSG_DESADV_VAG': 'varchar(max)', 'MSG_EXTORD_FORMAT': 'varchar(max)', 'MSG_EXTORD_PA': 'varchar(max)', 'MSG_EXTORD_TRANSPORT': 'varchar(max)', 'MSG_EXTORD_VAG': 'varchar(max)', 'MSG_INVOIC_FORMAT': 'varchar(max)', 'MSG_INVOIC_PA': 'varchar(max)', 'MSG_INVOIC_TRANSPORT': 'varchar(max)', 'MSG_INVOIC_VAG': 'varchar(max)', 'MSG_MSCONS_FORMAT': 'varchar(max)', 'MSG_MSCONS_PA': 'varchar(max)', 'MSG_MSCONS_TRANSPORT': 'varchar(max)', 'MSG_MSCONS_VAG': 'varchar(max)', 'MSG_ORDERS_FORMAT': 'varchar(max)', 'MSG_ORDERS_PA': 'varchar(max)', 'MSG_ORDERS_TRANSPORT': 'varchar(max)', 'MSG_ORDERS_VAG': 'varchar(max)', 'MSG_ORDRSP_FORMAT': 'varchar(max)', 'MSG_ORDRSP_PA': 'varchar(max)', 'MSG_ORDRSP_TRANSPORT': 'varchar(max)', 'MSG_ORDRSP_VAG': 'varchar(max)', 'MSG_PRICAT_FORMAT': 'varchar(max)', 'MSG_PRICAT_PA': 'varchar(max)', 'MSG_PRICAT_TRANSPORT': 'varchar(max)', 'MSG_PRICAT_VAG': 'varchar(max)', 'MSG_SHOPCART_FORMAT': 'varchar(max)', 'MSG_SHOPCART_PA': 'varchar(max)', 'MSG_SHOPCART_TRANSPORT': 'varchar(max)', 'MSG_SHOPCART_VAG': 'varchar(max)', 'NAMN2': 'varchar(max)', 'NATCLEARKOD': 'varchar(max)', 'OMSATTNING': 'varchar(max)', 'OMSATTNINGSAR': 'varchar(max)', 'OMSATTNING_FG': 'varchar(max)', 'ORGNR': 'varchar(max)', 'ORT': 'varchar(max)', 'PASSIV': 'varchar(max)', 'PGNR': 'varchar(max)', 'SALDO': 'varchar(max)', 'SEPA': 'varchar(max)', 'SOKBEGR': 'varchar(max)', 'SWIFTADR': 'varchar(max)', 'TELNR': 'varchar(max)', 'TERMIN': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CAST(ADR1 AS VARCHAR(MAX)) AS adr1,
		CAST(ADR2 AS VARCHAR(MAX)) AS adr2,
		CAST(ATTRIBUTE_ACTORID AS VARCHAR(MAX)) AS attribute_actorid,
		CAST(ATTRIBUTE_EAN AS VARCHAR(MAX)) AS attribute_ean,
		CAST(BANKADR AS VARCHAR(MAX)) AS bankadr,
		CAST(BANKKONTO AS VARCHAR(MAX)) AS bankkonto,
		CAST(BANKLAND AS VARCHAR(MAX)) AS bankland,
		CAST(BANKNAMN AS VARCHAR(MAX)) AS banknamn,
		CAST(BANKORT AS VARCHAR(MAX)) AS bankort,
		CAST(BBAN AS VARCHAR(MAX)) AS bban,
		CAST(BETALVAG AS VARCHAR(MAX)) AS betalvag,
		CAST(BETSPARR AS VARCHAR(MAX)) AS betsparr,
		CAST(BGNR AS VARCHAR(MAX)) AS bgnr,
		CAST(BIC AS VARCHAR(MAX)) AS bic,
		CAST(DUMMY1 AS VARCHAR(MAX)) AS dummy1,
		CAST(ELEKTRONISK_KOMMUNIKATION AS VARCHAR(MAX)) AS elektronisk_kommunikation,
		CAST(ENGANG AS VARCHAR(MAX)) AS engang,
		CAST(FACBG AS VARCHAR(MAX)) AS facbg,
		CAST(FACNAMN AS VARCHAR(MAX)) AS facnamn,
		CAST(FACPG AS VARCHAR(MAX)) AS facpg,
		CAST(FRI1 AS VARCHAR(MAX)) AS fri1,
		CAST(FRI2 AS VARCHAR(MAX)) AS fri2,
		CAST(FRI3 AS VARCHAR(MAX)) AS fri3,
		CAST(FRI4 AS VARCHAR(MAX)) AS fri4,
		CAST(GRUPP AS VARCHAR(MAX)) AS grupp,
		CAST(IBAN AS VARCHAR(MAX)) AS iban,
		CAST(KREDITLIMIT AS VARCHAR(MAX)) AS kreditlimit,
		CAST(LAND AS VARCHAR(MAX)) AS land,
		CAST(LEVID AS VARCHAR(MAX)) AS levid,
		CAST(LEVID2 AS VARCHAR(MAX)) AS levid2,
		CAST(LEVID_ID_TEXT AS VARCHAR(MAX)) AS levid_id_text,
		CAST(LEVID_TEXT AS VARCHAR(MAX)) AS levid_text,
		CAST(LEV_EREF AS VARCHAR(MAX)) AS lev_eref,
		CAST(LEV_TAB_AVTRAM AS VARCHAR(MAX)) AS lev_tab_avtram,
		CAST(LEV_TAB_BETV AS VARCHAR(MAX)) AS lev_tab_betv,
		CAST(LEV_TAB_BEVAK AS VARCHAR(MAX)) AS lev_tab_bevak,
		CAST(LEV_TAB_CMALL AS VARCHAR(MAX)) AS lev_tab_cmall,
		CAST(LEV_TAB_FÖRK AS VARCHAR(MAX)) AS lev_tab_förk,
		CAST(LEV_TAB_MOMS AS VARCHAR(MAX)) AS lev_tab_moms,
		CAST(LEV_TAB_MOTP AS VARCHAR(MAX)) AS lev_tab_motp,
		CAST(LEV_TAB_REF AS VARCHAR(MAX)) AS lev_tab_ref,
		CAST(LEV_TAB_RESENH AS VARCHAR(MAX)) AS lev_tab_resenh,
		CAST(LEV_TAB_RESK AS VARCHAR(MAX)) AS lev_tab_resk,
		CAST(LEV_TAB_SCADAT AS VARCHAR(MAX)) AS lev_tab_scadat,
		CAST(LEV_TAB_SCANNR AS VARCHAR(MAX)) AS lev_tab_scannr,
		CAST(LEV_TAB_UBF AS VARCHAR(MAX)) AS lev_tab_ubf,
		CAST(LEV_TAB_UBK AS VARCHAR(MAX)) AS lev_tab_ubk,
		CAST(LEV_TAB_VALUTA AS VARCHAR(MAX)) AS lev_tab_valuta,
		CAST(LEV_VREF AS VARCHAR(MAX)) AS lev_vref,
		CAST(MOMSREG AS VARCHAR(MAX)) AS momsreg,
		CAST(MSG_DESADV_FORMAT AS VARCHAR(MAX)) AS msg_desadv_format,
		CAST(MSG_DESADV_PA AS VARCHAR(MAX)) AS msg_desadv_pa,
		CAST(MSG_DESADV_TRANSPORT AS VARCHAR(MAX)) AS msg_desadv_transport,
		CAST(MSG_DESADV_VAG AS VARCHAR(MAX)) AS msg_desadv_vag,
		CAST(MSG_EXTORD_FORMAT AS VARCHAR(MAX)) AS msg_extord_format,
		CAST(MSG_EXTORD_PA AS VARCHAR(MAX)) AS msg_extord_pa,
		CAST(MSG_EXTORD_TRANSPORT AS VARCHAR(MAX)) AS msg_extord_transport,
		CAST(MSG_EXTORD_VAG AS VARCHAR(MAX)) AS msg_extord_vag,
		CAST(MSG_INVOIC_FORMAT AS VARCHAR(MAX)) AS msg_invoic_format,
		CAST(MSG_INVOIC_PA AS VARCHAR(MAX)) AS msg_invoic_pa,
		CAST(MSG_INVOIC_TRANSPORT AS VARCHAR(MAX)) AS msg_invoic_transport,
		CAST(MSG_INVOIC_VAG AS VARCHAR(MAX)) AS msg_invoic_vag,
		CAST(MSG_MSCONS_FORMAT AS VARCHAR(MAX)) AS msg_mscons_format,
		CAST(MSG_MSCONS_PA AS VARCHAR(MAX)) AS msg_mscons_pa,
		CAST(MSG_MSCONS_TRANSPORT AS VARCHAR(MAX)) AS msg_mscons_transport,
		CAST(MSG_MSCONS_VAG AS VARCHAR(MAX)) AS msg_mscons_vag,
		CAST(MSG_ORDERS_FORMAT AS VARCHAR(MAX)) AS msg_orders_format,
		CAST(MSG_ORDERS_PA AS VARCHAR(MAX)) AS msg_orders_pa,
		CAST(MSG_ORDERS_TRANSPORT AS VARCHAR(MAX)) AS msg_orders_transport,
		CAST(MSG_ORDERS_VAG AS VARCHAR(MAX)) AS msg_orders_vag,
		CAST(MSG_ORDRSP_FORMAT AS VARCHAR(MAX)) AS msg_ordrsp_format,
		CAST(MSG_ORDRSP_PA AS VARCHAR(MAX)) AS msg_ordrsp_pa,
		CAST(MSG_ORDRSP_TRANSPORT AS VARCHAR(MAX)) AS msg_ordrsp_transport,
		CAST(MSG_ORDRSP_VAG AS VARCHAR(MAX)) AS msg_ordrsp_vag,
		CAST(MSG_PRICAT_FORMAT AS VARCHAR(MAX)) AS msg_pricat_format,
		CAST(MSG_PRICAT_PA AS VARCHAR(MAX)) AS msg_pricat_pa,
		CAST(MSG_PRICAT_TRANSPORT AS VARCHAR(MAX)) AS msg_pricat_transport,
		CAST(MSG_PRICAT_VAG AS VARCHAR(MAX)) AS msg_pricat_vag,
		CAST(MSG_SHOPCART_FORMAT AS VARCHAR(MAX)) AS msg_shopcart_format,
		CAST(MSG_SHOPCART_PA AS VARCHAR(MAX)) AS msg_shopcart_pa,
		CAST(MSG_SHOPCART_TRANSPORT AS VARCHAR(MAX)) AS msg_shopcart_transport,
		CAST(MSG_SHOPCART_VAG AS VARCHAR(MAX)) AS msg_shopcart_vag,
		CAST(NAMN2 AS VARCHAR(MAX)) AS namn2,
		CAST(NATCLEARKOD AS VARCHAR(MAX)) AS natclearkod,
		CAST(OMSATTNING AS VARCHAR(MAX)) AS omsattning,
		CONVERT(varchar(max), OMSATTNINGSAR, 126) AS omsattningsar,
		CAST(OMSATTNING_FG AS VARCHAR(MAX)) AS omsattning_fg,
		CAST(ORGNR AS VARCHAR(MAX)) AS orgnr,
		CAST(ORT AS VARCHAR(MAX)) AS ort,
		CAST(PASSIV AS VARCHAR(MAX)) AS passiv,
		CAST(PGNR AS VARCHAR(MAX)) AS pgnr,
		CAST(SALDO AS VARCHAR(MAX)) AS saldo,
		CAST(SEPA AS VARCHAR(MAX)) AS sepa,
		CAST(SOKBEGR AS VARCHAR(MAX)) AS sokbegr,
		CAST(SWIFTADR AS VARCHAR(MAX)) AS swiftadr,
		CAST(TELNR AS VARCHAR(MAX)) AS telnr,
		CAST(TERMIN AS VARCHAR(MAX)) AS termin 
	FROM utdata.utdata298.RK_DIM_LEV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
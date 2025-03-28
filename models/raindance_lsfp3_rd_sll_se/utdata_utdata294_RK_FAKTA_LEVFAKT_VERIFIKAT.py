
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTEST': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVTALID': 'varchar(max)', 'AVTBES_ID': 'varchar(max)', 'BELOPP_SEK': 'varchar(max)', 'BELOPP_VAL': 'varchar(max)', 'BETALNINGSSPARR': 'varchar(max)', 'BETALT_SEK': 'varchar(max)', 'BETALT_VAL': 'varchar(max)', 'BETDAGAR': 'varchar(max)', 'BPUTF_V': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'EREF': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTURADATUM': 'varchar(max)', 'FAKTURA_REGDATUM': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FORFALLODATUM': 'varchar(max)', 'FRI1_ID': 'varchar(max)', 'FRI2_ID': 'varchar(max)', 'F_DOK_ANTAL': 'varchar(max)', 'F_MED': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'IB': 'varchar(max)', 'ID_ID': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KRAVNIVA': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LEVID': 'varchar(max)', 'LEVRTYP': 'varchar(max)', 'MED': 'varchar(max)', 'MOMS_SEK': 'varchar(max)', 'MOMS_VAL': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTTATTDAT': 'varchar(max)', 'MOTTATTSIGN': 'varchar(max)', 'NR': 'varchar(max)', 'OCRNR': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'OVERDRDAGAR': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PRG_V': 'varchar(max)', 'PROC_ID': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'RANTEDEB': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'STATUS': 'varchar(max)', 'TAB_BETV': 'varchar(max)', 'TAB_CMALL': 'varchar(max)', 'TAB_FÖRK': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_REF': 'varchar(max)', 'TAB_RESENH': 'varchar(max)', 'TAB_RESK': 'varchar(max)', 'TAB_SCADAT': 'varchar(max)', 'TAB_SCANNR': 'varchar(max)', 'TAB_UBF': 'varchar(max)', 'TAB_UBK': 'varchar(max)', 'TAB_VALUTA': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'URS_ID': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'UTSKRDATUM': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'VREF': 'varchar(max)'},
    
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
		'lsfp3_rd_sll_se_utdata_utdata294' as _source,
		CAST(ATTEST AS VARCHAR(MAX)) AS ATTEST,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS ATTESTDATUM1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS ATTESTDATUM2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(AVTALID AS VARCHAR(MAX)) AS AVTALID,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
		CAST(BELOPP_SEK AS VARCHAR(MAX)) AS BELOPP_SEK,
		CAST(BELOPP_VAL AS VARCHAR(MAX)) AS BELOPP_VAL,
		CAST(BETALNINGSSPARR AS VARCHAR(MAX)) AS BETALNINGSSPARR,
		CAST(BETALT_SEK AS VARCHAR(MAX)) AS BETALT_SEK,
		CAST(BETALT_VAL AS VARCHAR(MAX)) AS BETALT_VAL,
		CAST(BETDAGAR AS VARCHAR(MAX)) AS BETDAGAR,
		CAST(BPUTF_V AS VARCHAR(MAX)) AS BPUTF_V,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS BUDGET_V,
		CONVERT(varchar(max), DEFDATUM, 126) AS DEFDATUM,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
		CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS DOK_ANTAL,
		CAST(EREF AS VARCHAR(MAX)) AS EREF,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS EXTERNANM,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS EXTERNNR,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS FAKTSTATUS,
		CONVERT(varchar(max), FAKTURADATUM, 126) AS FAKTURADATUM,
		CONVERT(varchar(max), FAKTURA_REGDATUM, 126) AS FAKTURA_REGDATUM,
		CAST(FORETAG AS VARCHAR(MAX)) AS FORETAG,
		CONVERT(varchar(max), FORFALLODATUM, 126) AS FORFALLODATUM,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
		CAST(F_DOK_ANTAL AS VARCHAR(MAX)) AS F_DOK_ANTAL,
		CAST(F_MED AS VARCHAR(MAX)) AS F_MED,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS FÖRBEL_V,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
		CAST(IB AS VARCHAR(MAX)) AS IB,
		CAST(ID_ID AS VARCHAR(MAX)) AS ID_ID,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(KATEGORI AS VARCHAR(MAX)) AS KATEGORI,
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
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS ORGVAL_V,
		CAST(OVERDRDAGAR AS VARCHAR(MAX)) AS OVERDRDAGAR,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
		CAST(PRG_V AS VARCHAR(MAX)) AS PRG_V,
		CAST(PROC_ID AS VARCHAR(MAX)) AS PROC_ID,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(RADTEXT AS VARCHAR(MAX)) AS RADTEXT,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS RADTYPNR,
		CAST(RANTEDEB AS VARCHAR(MAX)) AS RANTEDEB,
		CONVERT(varchar(max), REGDATUM, 126) AS REGDATUM,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS SENASTBETDATUM,
		CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
		CAST(TAB_BETV AS VARCHAR(MAX)) AS TAB_BETV,
		CAST(TAB_CMALL AS VARCHAR(MAX)) AS TAB_CMALL,
		CAST(TAB_FÖRK AS VARCHAR(MAX)) AS TAB_FÖRK,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS TAB_MOMS,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS TAB_MOTP,
		CAST(TAB_REF AS VARCHAR(MAX)) AS TAB_REF,
		CAST(TAB_RESENH AS VARCHAR(MAX)) AS TAB_RESENH,
		CAST(TAB_RESK AS VARCHAR(MAX)) AS TAB_RESK,
		CAST(TAB_SCADAT AS VARCHAR(MAX)) AS TAB_SCADAT,
		CAST(TAB_SCANNR AS VARCHAR(MAX)) AS TAB_SCANNR,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS TAB_UBF,
		CAST(TAB_UBK AS VARCHAR(MAX)) AS TAB_UBK,
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS TAB_VALUTA,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS URSPRUNGS_VERIFIKAT,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS URSPTEXT,
		CAST(URS_ID AS VARCHAR(MAX)) AS URS_ID,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
		CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
		CONVERT(varchar(max), UTSKRDATUM, 126) AS UTSKRDATUM,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS VALUTA_ID,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS VERDOKREF,
		CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID,
		CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
		CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD,
		CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
		CAST(VREF AS VARCHAR(MAX)) AS VREF 
	FROM utdata.utdata294.RK_FAKTA_LEVFAKT_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
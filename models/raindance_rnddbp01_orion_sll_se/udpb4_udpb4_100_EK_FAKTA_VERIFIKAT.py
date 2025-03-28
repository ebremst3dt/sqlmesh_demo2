
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AKT_ID': 'varchar(max)', 'ATTESTDATUM1': 'varchar(max)', 'ATTESTDATUM2': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN2': 'varchar(max)', 'AVT_ID': 'varchar(max)', 'BESTUT_V': 'varchar(max)', 'BUDGET_V': 'varchar(max)', 'BUDV2_V': 'varchar(max)', 'DATUM_ID': 'varchar(max)', 'DEFDATUM': 'varchar(max)', 'DEFSIGN': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)', 'DOK_ANTAL': 'varchar(max)', 'ERS_ID': 'varchar(max)', 'EXTERNANM': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNNR': 'varchar(max)', 'FORETAG': 'varchar(max)', 'FÖPROC_ID': 'varchar(max)', 'FÖRBEL_V': 'varchar(max)', 'HUVUDTEXT': 'varchar(max)', 'HÄND_ID': 'varchar(max)', 'IB': 'varchar(max)', 'IMOT_ID': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'KASSA_ID': 'varchar(max)', 'KATEGORI': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KST_ID': 'varchar(max)', 'MED': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'ORGVAL_V': 'varchar(max)', 'PNYCKEL': 'varchar(max)', 'PROG_V': 'varchar(max)', 'PROR12_V': 'varchar(max)', 'RADTEXT': 'varchar(max)', 'RADTYPNR': 'varchar(max)', 'REGDATUM': 'varchar(max)', 'REGDAT_ID': 'varchar(max)', 'REGSIGN': 'varchar(max)', 'SID_ID': 'varchar(max)', 'STATUS': 'varchar(max)', 'TRID_ID': 'varchar(max)', 'URSPRUNGS_VERIFIKAT': 'varchar(max)', 'URSPTEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTFUTL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VAL_ID': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERDOKREF': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)', 'VERTYP': 'varchar(max)', 'YRK_ID': 'varchar(max)'},
    
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CAST(AKT_ID AS VARCHAR(MAX)) AS AKT_ID,
		CONVERT(varchar(max), ATTESTDATUM1, 126) AS ATTESTDATUM1,
		CONVERT(varchar(max), ATTESTDATUM2, 126) AS ATTESTDATUM2,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
		CAST(AVT_ID AS VARCHAR(MAX)) AS AVT_ID,
		CAST(BESTUT_V AS VARCHAR(MAX)) AS BESTUT_V,
		CAST(BUDGET_V AS VARCHAR(MAX)) AS BUDGET_V,
		CAST(BUDV2_V AS VARCHAR(MAX)) AS BUDV2_V,
		CAST(DATUM_ID AS VARCHAR(MAX)) AS DATUM_ID,
		CONVERT(varchar(max), DEFDATUM, 126) AS DEFDATUM,
		CAST(DEFSIGN AS VARCHAR(MAX)) AS DEFSIGN,
		CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID,
		CAST(DOK_ANTAL AS VARCHAR(MAX)) AS DOK_ANTAL,
		CAST(ERS_ID AS VARCHAR(MAX)) AS ERS_ID,
		CAST(EXTERNANM AS VARCHAR(MAX)) AS EXTERNANM,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNNR AS VARCHAR(MAX)) AS EXTERNNR,
		CAST(FORETAG AS VARCHAR(MAX)) AS FORETAG,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
		CAST(FÖRBEL_V AS VARCHAR(MAX)) AS FÖRBEL_V,
		CAST(HUVUDTEXT AS VARCHAR(MAX)) AS HUVUDTEXT,
		CAST(HÄND_ID AS VARCHAR(MAX)) AS HÄND_ID,
		CAST(IB AS VARCHAR(MAX)) AS IB,
		CAST(IMOT_ID AS VARCHAR(MAX)) AS IMOT_ID,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
		CAST(KASSA_ID AS VARCHAR(MAX)) AS KASSA_ID,
		CAST(KATEGORI AS VARCHAR(MAX)) AS KATEGORI,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS KONTSIGN,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(MED AS VARCHAR(MAX)) AS MED,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(ORGVAL_V AS VARCHAR(MAX)) AS ORGVAL_V,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
		CAST(PROG_V AS VARCHAR(MAX)) AS PROG_V,
		CAST(PROR12_V AS VARCHAR(MAX)) AS PROR12_V,
		CAST(RADTEXT AS VARCHAR(MAX)) AS RADTEXT,
		CAST(RADTYPNR AS VARCHAR(MAX)) AS RADTYPNR,
		CONVERT(varchar(max), REGDATUM, 126) AS REGDATUM,
		CAST(REGDAT_ID AS VARCHAR(MAX)) AS REGDAT_ID,
		CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
		CAST(SID_ID AS VARCHAR(MAX)) AS SID_ID,
		CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
		CAST(TRID_ID AS VARCHAR(MAX)) AS TRID_ID,
		CAST(URSPRUNGS_VERIFIKAT AS VARCHAR(MAX)) AS URSPRUNGS_VERIFIKAT,
		CAST(URSPTEXT AS VARCHAR(MAX)) AS URSPTEXT,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
		CAST(UTFUTL_V AS VARCHAR(MAX)) AS UTFUTL_V,
		CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
		CAST(VAL_ID AS VARCHAR(MAX)) AS VAL_ID,
		CONVERT(varchar(max), VERDATUM, 126) AS VERDATUM,
		CAST(VERDOKREF AS VARCHAR(MAX)) AS VERDOKREF,
		CAST(VERNR AS VARCHAR(MAX)) AS VERNR,
		CAST(VERRAD AS VARCHAR(MAX)) AS VERRAD,
		CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
		CAST(YRK_ID AS VARCHAR(MAX)) AS YRK_ID 
	FROM udpb4.udpb4_100.EK_FAKTA_VERIFIKAT ) y
WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    
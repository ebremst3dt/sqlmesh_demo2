
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'HKRAD_GILTIG_FOM': 'varchar(max)', 'HKRAD_GILTIG_TOM': 'varchar(max)', 'HKRAD_ID': 'varchar(max)', 'HKRAD_ID_TEXT': 'varchar(max)', 'HKRAD_PASSIV': 'varchar(max)', 'HKRAD_TEXT': 'varchar(max)', 'KGR_GILTIG_FOM': 'varchar(max)', 'KGR_GILTIG_TOM': 'varchar(max)', 'KGR_ID': 'varchar(max)', 'KGR_ID_TEXT': 'varchar(max)', 'KGR_PASSIV': 'varchar(max)', 'KGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KLIRAD_GILTIG_FOM': 'varchar(max)', 'KLIRAD_GILTIG_TOM': 'varchar(max)', 'KLIRAD_ID': 'varchar(max)', 'KLIRAD_ID_TEXT': 'varchar(max)', 'KLIRAD_PASSIV': 'varchar(max)', 'KLIRAD_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'SRU_GILTIG_FOM': 'varchar(max)', 'SRU_GILTIG_TOM': 'varchar(max)', 'SRU_ID': 'varchar(max)', 'SRU_ID_TEXT': 'varchar(max)', 'SRU_PASSIV': 'varchar(max)', 'SRU_TEXT': 'varchar(max)', 'STYRAD_GILTIG_FOM': 'varchar(max)', 'STYRAD_GILTIG_TOM': 'varchar(max)', 'STYRAD_ID': 'varchar(max)', 'STYRAD_ID_TEXT': 'varchar(max)', 'STYRAD_PASSIV': 'varchar(max)', 'STYRAD_TEXT': 'varchar(max)'},
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
		'ftvddbs08_ftv_sll_se_ftvudp_ftv_400' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), HKRAD_GILTIG_FOM, 126) AS HKRAD_GILTIG_FOM,
		CONVERT(varchar(max), HKRAD_GILTIG_TOM, 126) AS HKRAD_GILTIG_TOM,
		CAST(HKRAD_ID AS VARCHAR(MAX)) AS HKRAD_ID,
		CAST(HKRAD_ID_TEXT AS VARCHAR(MAX)) AS HKRAD_ID_TEXT,
		CAST(HKRAD_PASSIV AS VARCHAR(MAX)) AS HKRAD_PASSIV,
		CAST(HKRAD_TEXT AS VARCHAR(MAX)) AS HKRAD_TEXT,
		CONVERT(varchar(max), KGR_GILTIG_FOM, 126) AS KGR_GILTIG_FOM,
		CONVERT(varchar(max), KGR_GILTIG_TOM, 126) AS KGR_GILTIG_TOM,
		CAST(KGR_ID AS VARCHAR(MAX)) AS KGR_ID,
		CAST(KGR_ID_TEXT AS VARCHAR(MAX)) AS KGR_ID_TEXT,
		CAST(KGR_PASSIV AS VARCHAR(MAX)) AS KGR_PASSIV,
		CAST(KGR_TEXT AS VARCHAR(MAX)) AS KGR_TEXT,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS KKL_GILTIG_FOM,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS KKL_GILTIG_TOM,
		CAST(KKL_ID AS VARCHAR(MAX)) AS KKL_ID,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS KKL_ID_TEXT,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS KKL_PASSIV,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS KKL_TEXT,
		CONVERT(varchar(max), KLIRAD_GILTIG_FOM, 126) AS KLIRAD_GILTIG_FOM,
		CONVERT(varchar(max), KLIRAD_GILTIG_TOM, 126) AS KLIRAD_GILTIG_TOM,
		CAST(KLIRAD_ID AS VARCHAR(MAX)) AS KLIRAD_ID,
		CAST(KLIRAD_ID_TEXT AS VARCHAR(MAX)) AS KLIRAD_ID_TEXT,
		CAST(KLIRAD_PASSIV AS VARCHAR(MAX)) AS KLIRAD_PASSIV,
		CAST(KLIRAD_TEXT AS VARCHAR(MAX)) AS KLIRAD_TEXT,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS KONTO_GILTIG_FOM,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS KONTO_GILTIG_TOM,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS KONTO_ID_TEXT,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS KONTO_PASSIV,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS KONTO_TEXT,
		CONVERT(varchar(max), SRU_GILTIG_FOM, 126) AS SRU_GILTIG_FOM,
		CONVERT(varchar(max), SRU_GILTIG_TOM, 126) AS SRU_GILTIG_TOM,
		CAST(SRU_ID AS VARCHAR(MAX)) AS SRU_ID,
		CAST(SRU_ID_TEXT AS VARCHAR(MAX)) AS SRU_ID_TEXT,
		CAST(SRU_PASSIV AS VARCHAR(MAX)) AS SRU_PASSIV,
		CAST(SRU_TEXT AS VARCHAR(MAX)) AS SRU_TEXT,
		CONVERT(varchar(max), STYRAD_GILTIG_FOM, 126) AS STYRAD_GILTIG_FOM,
		CONVERT(varchar(max), STYRAD_GILTIG_TOM, 126) AS STYRAD_GILTIG_TOM,
		CAST(STYRAD_ID AS VARCHAR(MAX)) AS STYRAD_ID,
		CAST(STYRAD_ID_TEXT AS VARCHAR(MAX)) AS STYRAD_ID_TEXT,
		CAST(STYRAD_PASSIV AS VARCHAR(MAX)) AS STYRAD_PASSIV,
		CAST(STYRAD_TEXT AS VARCHAR(MAX)) AS STYRAD_TEXT 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    
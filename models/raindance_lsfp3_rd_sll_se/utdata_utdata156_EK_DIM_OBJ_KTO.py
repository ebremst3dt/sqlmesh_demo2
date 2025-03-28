
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKGR_GILTIG_FOM': 'varchar(max)', 'KKGR_GILTIG_TOM': 'varchar(max)', 'KKGR_ID': 'varchar(max)', 'KKGR_ID_TEXT': 'varchar(max)', 'KKGR_PASSIV': 'varchar(max)', 'KKGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KK_GILTIG_FOM': 'varchar(max)', 'KK_GILTIG_TOM': 'varchar(max)', 'KK_ID': 'varchar(max)', 'KK_ID_TEXT': 'varchar(max)', 'KK_PASSIV': 'varchar(max)', 'KK_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)', 'R3_GILTIG_FOM': 'varchar(max)', 'R3_GILTIG_TOM': 'varchar(max)', 'R3_ID': 'varchar(max)', 'R3_ID_TEXT': 'varchar(max)', 'R3_PASSIV': 'varchar(max)', 'R3_TEXT': 'varchar(max)', 'R4_GILTIG_FOM': 'varchar(max)', 'R4_GILTIG_TOM': 'varchar(max)', 'R4_ID': 'varchar(max)', 'R4_ID_TEXT': 'varchar(max)', 'R4_PASSIV': 'varchar(max)', 'R4_TEXT': 'varchar(max)', 'RRBR_GILTIG_FOM': 'varchar(max)', 'RRBR_GILTIG_TOM': 'varchar(max)', 'RRBR_ID': 'varchar(max)', 'RRBR_ID_TEXT': 'varchar(max)', 'RRBR_PASSIV': 'varchar(max)', 'RRBR_TEXT': 'varchar(max)', 'STÖKGR_GILTIG_FOM': 'varchar(max)', 'STÖKGR_GILTIG_TOM': 'varchar(max)', 'STÖKGR_ID': 'varchar(max)', 'STÖKGR_ID_TEXT': 'varchar(max)', 'STÖKGR_PASSIV': 'varchar(max)', 'STÖKGR_TEXT': 'varchar(max)', 'STÖKOD_GILTIG_FOM': 'varchar(max)', 'STÖKOD_GILTIG_TOM': 'varchar(max)', 'STÖKOD_ID': 'varchar(max)', 'STÖKOD_ID_TEXT': 'varchar(max)', 'STÖKOD_PASSIV': 'varchar(max)', 'STÖKOD_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata156' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS KGRUPP_GILTIG_FOM,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS KGRUPP_GILTIG_TOM,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS KGRUPP_ID,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS KGRUPP_ID_TEXT,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS KGRUPP_PASSIV,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS KGRUPP_TEXT,
		CONVERT(varchar(max), KKGR_GILTIG_FOM, 126) AS KKGR_GILTIG_FOM,
		CONVERT(varchar(max), KKGR_GILTIG_TOM, 126) AS KKGR_GILTIG_TOM,
		CAST(KKGR_ID AS VARCHAR(MAX)) AS KKGR_ID,
		CAST(KKGR_ID_TEXT AS VARCHAR(MAX)) AS KKGR_ID_TEXT,
		CAST(KKGR_PASSIV AS VARCHAR(MAX)) AS KKGR_PASSIV,
		CAST(KKGR_TEXT AS VARCHAR(MAX)) AS KKGR_TEXT,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS KKL_GILTIG_FOM,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS KKL_GILTIG_TOM,
		CAST(KKL_ID AS VARCHAR(MAX)) AS KKL_ID,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS KKL_ID_TEXT,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS KKL_PASSIV,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS KKL_TEXT,
		CONVERT(varchar(max), KK_GILTIG_FOM, 126) AS KK_GILTIG_FOM,
		CONVERT(varchar(max), KK_GILTIG_TOM, 126) AS KK_GILTIG_TOM,
		CAST(KK_ID AS VARCHAR(MAX)) AS KK_ID,
		CAST(KK_ID_TEXT AS VARCHAR(MAX)) AS KK_ID_TEXT,
		CAST(KK_PASSIV AS VARCHAR(MAX)) AS KK_PASSIV,
		CAST(KK_TEXT AS VARCHAR(MAX)) AS KK_TEXT,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS KTO_GILTIG_FOM,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS KTO_GILTIG_TOM,
		CAST(KTO_ID AS VARCHAR(MAX)) AS KTO_ID,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS KTO_ID_TEXT,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS KTO_PASSIV,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS KTO_TEXT,
		CONVERT(varchar(max), R3_GILTIG_FOM, 126) AS R3_GILTIG_FOM,
		CONVERT(varchar(max), R3_GILTIG_TOM, 126) AS R3_GILTIG_TOM,
		CAST(R3_ID AS VARCHAR(MAX)) AS R3_ID,
		CAST(R3_ID_TEXT AS VARCHAR(MAX)) AS R3_ID_TEXT,
		CAST(R3_PASSIV AS VARCHAR(MAX)) AS R3_PASSIV,
		CAST(R3_TEXT AS VARCHAR(MAX)) AS R3_TEXT,
		CONVERT(varchar(max), R4_GILTIG_FOM, 126) AS R4_GILTIG_FOM,
		CONVERT(varchar(max), R4_GILTIG_TOM, 126) AS R4_GILTIG_TOM,
		CAST(R4_ID AS VARCHAR(MAX)) AS R4_ID,
		CAST(R4_ID_TEXT AS VARCHAR(MAX)) AS R4_ID_TEXT,
		CAST(R4_PASSIV AS VARCHAR(MAX)) AS R4_PASSIV,
		CAST(R4_TEXT AS VARCHAR(MAX)) AS R4_TEXT,
		CONVERT(varchar(max), RRBR_GILTIG_FOM, 126) AS RRBR_GILTIG_FOM,
		CONVERT(varchar(max), RRBR_GILTIG_TOM, 126) AS RRBR_GILTIG_TOM,
		CAST(RRBR_ID AS VARCHAR(MAX)) AS RRBR_ID,
		CAST(RRBR_ID_TEXT AS VARCHAR(MAX)) AS RRBR_ID_TEXT,
		CAST(RRBR_PASSIV AS VARCHAR(MAX)) AS RRBR_PASSIV,
		CAST(RRBR_TEXT AS VARCHAR(MAX)) AS RRBR_TEXT,
		CONVERT(varchar(max), STÖKGR_GILTIG_FOM, 126) AS STÖKGR_GILTIG_FOM,
		CONVERT(varchar(max), STÖKGR_GILTIG_TOM, 126) AS STÖKGR_GILTIG_TOM,
		CAST(STÖKGR_ID AS VARCHAR(MAX)) AS STÖKGR_ID,
		CAST(STÖKGR_ID_TEXT AS VARCHAR(MAX)) AS STÖKGR_ID_TEXT,
		CAST(STÖKGR_PASSIV AS VARCHAR(MAX)) AS STÖKGR_PASSIV,
		CAST(STÖKGR_TEXT AS VARCHAR(MAX)) AS STÖKGR_TEXT,
		CONVERT(varchar(max), STÖKOD_GILTIG_FOM, 126) AS STÖKOD_GILTIG_FOM,
		CONVERT(varchar(max), STÖKOD_GILTIG_TOM, 126) AS STÖKOD_GILTIG_TOM,
		CAST(STÖKOD_ID AS VARCHAR(MAX)) AS STÖKOD_ID,
		CAST(STÖKOD_ID_TEXT AS VARCHAR(MAX)) AS STÖKOD_ID_TEXT,
		CAST(STÖKOD_PASSIV AS VARCHAR(MAX)) AS STÖKOD_PASSIV,
		CAST(STÖKOD_TEXT AS VARCHAR(MAX)) AS STÖKOD_TEXT,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS TSIK_GILTIG_FOM,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS TSIK_GILTIG_TOM,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS TSIK_ID,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS TSIK_ID_TEXT,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS TSIK_PASSIV,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS TSIK_TEXT 
	FROM utdata.utdata156.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
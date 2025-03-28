
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'GKTO_GILTIG_FOM': 'varchar(max)', 'GKTO_GILTIG_TOM': 'varchar(max)', 'GKTO_ID': 'varchar(max)', 'GKTO_ID_TEXT': 'varchar(max)', 'GKTO_PASSIV': 'varchar(max)', 'GKTO_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata840' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), GKTO_GILTIG_FOM, 126) AS GKTO_GILTIG_FOM,
		CONVERT(varchar(max), GKTO_GILTIG_TOM, 126) AS GKTO_GILTIG_TOM,
		CAST(GKTO_ID AS VARCHAR(MAX)) AS GKTO_ID,
		CAST(GKTO_ID_TEXT AS VARCHAR(MAX)) AS GKTO_ID_TEXT,
		CAST(GKTO_PASSIV AS VARCHAR(MAX)) AS GKTO_PASSIV,
		CAST(GKTO_TEXT AS VARCHAR(MAX)) AS GKTO_TEXT,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS KGRUPP_GILTIG_FOM,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS KGRUPP_GILTIG_TOM,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS KGRUPP_ID,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS KGRUPP_ID_TEXT,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS KGRUPP_PASSIV,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS KGRUPP_TEXT,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS KKL_GILTIG_FOM,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS KKL_GILTIG_TOM,
		CAST(KKL_ID AS VARCHAR(MAX)) AS KKL_ID,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS KKL_ID_TEXT,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS KKL_PASSIV,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS KKL_TEXT,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS KTO_GILTIG_FOM,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS KTO_GILTIG_TOM,
		CAST(KTO_ID AS VARCHAR(MAX)) AS KTO_ID,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS KTO_ID_TEXT,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS KTO_PASSIV,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS KTO_TEXT,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS TSIK_GILTIG_FOM,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS TSIK_GILTIG_TOM,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS TSIK_ID,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS TSIK_ID_TEXT,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS TSIK_PASSIV,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS TSIK_TEXT 
	FROM utdata.utdata840.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
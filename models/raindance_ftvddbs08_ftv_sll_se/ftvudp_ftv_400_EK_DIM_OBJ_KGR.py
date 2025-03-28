
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KGR_GILTIG_FOM': 'varchar(max)', 'KGR_GILTIG_TOM': 'varchar(max)', 'KGR_ID': 'varchar(max)', 'KGR_ID_TEXT': 'varchar(max)', 'KGR_PASSIV': 'varchar(max)', 'KGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)'},
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
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS KKL_TEXT 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_KGR ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    
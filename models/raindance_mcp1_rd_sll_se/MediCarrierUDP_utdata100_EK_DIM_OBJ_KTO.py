
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FR01_GILTIG_FOM': 'varchar(max)', 'FR01_GILTIG_TOM': 'varchar(max)', 'FR01_ID': 'varchar(max)', 'FR01_ID_TEXT': 'varchar(max)', 'FR01_PASSIV': 'varchar(max)', 'FR01_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KTOG_GILTIG_FOM': 'varchar(max)', 'KTOG_GILTIG_TOM': 'varchar(max)', 'KTOG_ID': 'varchar(max)', 'KTOG_ID_TEXT': 'varchar(max)', 'KTOG_PASSIV': 'varchar(max)', 'KTOG_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)'},
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
		'mcp1_rd_sll_se_MediCarrierUDP_utdata100' as _source,
		CONVERT(varchar(max), FR01_GILTIG_FOM, 126) AS FR01_GILTIG_FOM,
		CONVERT(varchar(max), FR01_GILTIG_TOM, 126) AS FR01_GILTIG_TOM,
		CAST(FR01_ID AS VARCHAR(MAX)) AS FR01_ID,
		CAST(FR01_ID_TEXT AS VARCHAR(MAX)) AS FR01_ID_TEXT,
		CAST(FR01_PASSIV AS VARCHAR(MAX)) AS FR01_PASSIV,
		CAST(FR01_TEXT AS VARCHAR(MAX)) AS FR01_TEXT,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS KKL_GILTIG_FOM,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS KKL_GILTIG_TOM,
		CAST(KKL_ID AS VARCHAR(MAX)) AS KKL_ID,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS KKL_ID_TEXT,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS KKL_PASSIV,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS KKL_TEXT,
		CONVERT(varchar(max), KTOG_GILTIG_FOM, 126) AS KTOG_GILTIG_FOM,
		CONVERT(varchar(max), KTOG_GILTIG_TOM, 126) AS KTOG_GILTIG_TOM,
		CAST(KTOG_ID AS VARCHAR(MAX)) AS KTOG_ID,
		CAST(KTOG_ID_TEXT AS VARCHAR(MAX)) AS KTOG_ID_TEXT,
		CAST(KTOG_PASSIV AS VARCHAR(MAX)) AS KTOG_PASSIV,
		CAST(KTOG_TEXT AS VARCHAR(MAX)) AS KTOG_TEXT,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS KTO_GILTIG_FOM,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS KTO_GILTIG_TOM,
		CAST(KTO_ID AS VARCHAR(MAX)) AS KTO_ID,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS KTO_ID_TEXT,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS KTO_PASSIV,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS KTO_TEXT 
	FROM MediCarrierUDP.utdata100.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    
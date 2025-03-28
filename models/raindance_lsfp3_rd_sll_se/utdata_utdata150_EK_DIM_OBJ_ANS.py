
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANS_GILTIG_FOM': 'varchar(max)', 'ANS_GILTIG_TOM': 'varchar(max)', 'ANS_ID': 'varchar(max)', 'ANS_ID_TEXT': 'varchar(max)', 'ANS_PASSIV': 'varchar(max)', 'ANS_TEXT': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)', 'FTG_GILTIG_FOM': 'varchar(max)', 'FTG_GILTIG_TOM': 'varchar(max)', 'FTG_ID': 'varchar(max)', 'FTG_ID_TEXT': 'varchar(max)', 'FTG_PASSIV': 'varchar(max)', 'FTG_TEXT': 'varchar(max)', 'RAM_GILTIG_FOM': 'varchar(max)', 'RAM_GILTIG_TOM': 'varchar(max)', 'RAM_ID': 'varchar(max)', 'RAM_ID_TEXT': 'varchar(max)', 'RAM_PASSIV': 'varchar(max)', 'RAM_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata150' as _source,
		CONVERT(varchar(max), ANS_GILTIG_FOM, 126) AS ANS_GILTIG_FOM,
		CONVERT(varchar(max), ANS_GILTIG_TOM, 126) AS ANS_GILTIG_TOM,
		CAST(ANS_ID AS VARCHAR(MAX)) AS ANS_ID,
		CAST(ANS_ID_TEXT AS VARCHAR(MAX)) AS ANS_ID_TEXT,
		CAST(ANS_PASSIV AS VARCHAR(MAX)) AS ANS_PASSIV,
		CAST(ANS_TEXT AS VARCHAR(MAX)) AS ANS_TEXT,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS AVD_GILTIG_FOM,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS AVD_GILTIG_TOM,
		CAST(AVD_ID AS VARCHAR(MAX)) AS AVD_ID,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS AVD_ID_TEXT,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS AVD_PASSIV,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS AVD_TEXT,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS ENH_GILTIG_FOM,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS ENH_GILTIG_TOM,
		CAST(ENH_ID AS VARCHAR(MAX)) AS ENH_ID,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS ENH_ID_TEXT,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS ENH_PASSIV,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS ENH_TEXT,
		CONVERT(varchar(max), FTG_GILTIG_FOM, 126) AS FTG_GILTIG_FOM,
		CONVERT(varchar(max), FTG_GILTIG_TOM, 126) AS FTG_GILTIG_TOM,
		CAST(FTG_ID AS VARCHAR(MAX)) AS FTG_ID,
		CAST(FTG_ID_TEXT AS VARCHAR(MAX)) AS FTG_ID_TEXT,
		CAST(FTG_PASSIV AS VARCHAR(MAX)) AS FTG_PASSIV,
		CAST(FTG_TEXT AS VARCHAR(MAX)) AS FTG_TEXT,
		CONVERT(varchar(max), RAM_GILTIG_FOM, 126) AS RAM_GILTIG_FOM,
		CONVERT(varchar(max), RAM_GILTIG_TOM, 126) AS RAM_GILTIG_TOM,
		CAST(RAM_ID AS VARCHAR(MAX)) AS RAM_ID,
		CAST(RAM_ID_TEXT AS VARCHAR(MAX)) AS RAM_ID_TEXT,
		CAST(RAM_PASSIV AS VARCHAR(MAX)) AS RAM_PASSIV,
		CAST(RAM_TEXT AS VARCHAR(MAX)) AS RAM_TEXT 
	FROM utdata.utdata150.EK_DIM_OBJ_ANS ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
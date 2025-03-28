
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'URSPR_GILTIG_FOM': 'varchar(max)', 'URSPR_GILTIG_TOM': 'varchar(max)', 'URSPR_ID': 'varchar(max)', 'URSPR_ID_TEXT': 'varchar(max)', 'URSPR_PASSIV': 'varchar(max)', 'URSPR_TEXT': 'varchar(max)', 'VTAM_GILTIG_FOM': 'varchar(max)', 'VTAM_GILTIG_TOM': 'varchar(max)', 'VTAM_ID': 'varchar(max)', 'VTAM_ID_TEXT': 'varchar(max)', 'VTAM_PASSIV': 'varchar(max)', 'VTAM_TEXT': 'varchar(max)', 'ÖVTYP_GILTIG_FOM': 'varchar(max)', 'ÖVTYP_GILTIG_TOM': 'varchar(max)', 'ÖVTYP_ID': 'varchar(max)', 'ÖVTYP_ID_TEXT': 'varchar(max)', 'ÖVTYP_PASSIV': 'varchar(max)', 'ÖVTYP_TEXT': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CONVERT(varchar(max), URSPR_GILTIG_FOM, 126) AS URSPR_GILTIG_FOM,
		CONVERT(varchar(max), URSPR_GILTIG_TOM, 126) AS URSPR_GILTIG_TOM,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS URSPR_ID,
		CAST(URSPR_ID_TEXT AS VARCHAR(MAX)) AS URSPR_ID_TEXT,
		CAST(URSPR_PASSIV AS VARCHAR(MAX)) AS URSPR_PASSIV,
		CAST(URSPR_TEXT AS VARCHAR(MAX)) AS URSPR_TEXT,
		CONVERT(varchar(max), VTAM_GILTIG_FOM, 126) AS VTAM_GILTIG_FOM,
		CONVERT(varchar(max), VTAM_GILTIG_TOM, 126) AS VTAM_GILTIG_TOM,
		CAST(VTAM_ID AS VARCHAR(MAX)) AS VTAM_ID,
		CAST(VTAM_ID_TEXT AS VARCHAR(MAX)) AS VTAM_ID_TEXT,
		CAST(VTAM_PASSIV AS VARCHAR(MAX)) AS VTAM_PASSIV,
		CAST(VTAM_TEXT AS VARCHAR(MAX)) AS VTAM_TEXT,
		CONVERT(varchar(max), ÖVTYP_GILTIG_FOM, 126) AS ÖVTYP_GILTIG_FOM,
		CONVERT(varchar(max), ÖVTYP_GILTIG_TOM, 126) AS ÖVTYP_GILTIG_TOM,
		CAST(ÖVTYP_ID AS VARCHAR(MAX)) AS ÖVTYP_ID,
		CAST(ÖVTYP_ID_TEXT AS VARCHAR(MAX)) AS ÖVTYP_ID_TEXT,
		CAST(ÖVTYP_PASSIV AS VARCHAR(MAX)) AS ÖVTYP_PASSIV,
		CAST(ÖVTYP_TEXT AS VARCHAR(MAX)) AS ÖVTYP_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_URSPR ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    
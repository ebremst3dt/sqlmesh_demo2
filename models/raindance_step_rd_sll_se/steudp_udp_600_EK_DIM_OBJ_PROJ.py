
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BE_GILTIG_FOM': 'varchar(max)', 'BE_GILTIG_TOM': 'varchar(max)', 'BE_ID': 'varchar(max)', 'BE_ID_TEXT': 'varchar(max)', 'BE_PASSIV': 'varchar(max)', 'BE_TEXT': 'varchar(max)', 'PANS_GILTIG_FOM': 'varchar(max)', 'PANS_GILTIG_TOM': 'varchar(max)', 'PANS_ID': 'varchar(max)', 'PANS_ID_TEXT': 'varchar(max)', 'PANS_PASSIV': 'varchar(max)', 'PANS_TEXT': 'varchar(max)', 'PKAT_GILTIG_FOM': 'varchar(max)', 'PKAT_GILTIG_TOM': 'varchar(max)', 'PKAT_ID': 'varchar(max)', 'PKAT_ID_TEXT': 'varchar(max)', 'PKAT_PASSIV': 'varchar(max)', 'PKAT_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)', 'PTYP_GILTIG_FOM': 'varchar(max)', 'PTYP_GILTIG_TOM': 'varchar(max)', 'PTYP_ID': 'varchar(max)', 'PTYP_ID_TEXT': 'varchar(max)', 'PTYP_PASSIV': 'varchar(max)', 'PTYP_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), BE_GILTIG_FOM, 126) AS BE_GILTIG_FOM,
		CONVERT(varchar(max), BE_GILTIG_TOM, 126) AS BE_GILTIG_TOM,
		CAST(BE_ID AS VARCHAR(MAX)) AS BE_ID,
		CAST(BE_ID_TEXT AS VARCHAR(MAX)) AS BE_ID_TEXT,
		CAST(BE_PASSIV AS VARCHAR(MAX)) AS BE_PASSIV,
		CAST(BE_TEXT AS VARCHAR(MAX)) AS BE_TEXT,
		CONVERT(varchar(max), PANS_GILTIG_FOM, 126) AS PANS_GILTIG_FOM,
		CONVERT(varchar(max), PANS_GILTIG_TOM, 126) AS PANS_GILTIG_TOM,
		CAST(PANS_ID AS VARCHAR(MAX)) AS PANS_ID,
		CAST(PANS_ID_TEXT AS VARCHAR(MAX)) AS PANS_ID_TEXT,
		CAST(PANS_PASSIV AS VARCHAR(MAX)) AS PANS_PASSIV,
		CAST(PANS_TEXT AS VARCHAR(MAX)) AS PANS_TEXT,
		CONVERT(varchar(max), PKAT_GILTIG_FOM, 126) AS PKAT_GILTIG_FOM,
		CONVERT(varchar(max), PKAT_GILTIG_TOM, 126) AS PKAT_GILTIG_TOM,
		CAST(PKAT_ID AS VARCHAR(MAX)) AS PKAT_ID,
		CAST(PKAT_ID_TEXT AS VARCHAR(MAX)) AS PKAT_ID_TEXT,
		CAST(PKAT_PASSIV AS VARCHAR(MAX)) AS PKAT_PASSIV,
		CAST(PKAT_TEXT AS VARCHAR(MAX)) AS PKAT_TEXT,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS PROJ_GILTIG_FOM,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS PROJ_GILTIG_TOM,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS PROJ_ID_TEXT,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS PROJ_PASSIV,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS PROJ_TEXT,
		CONVERT(varchar(max), PTYP_GILTIG_FOM, 126) AS PTYP_GILTIG_FOM,
		CONVERT(varchar(max), PTYP_GILTIG_TOM, 126) AS PTYP_GILTIG_TOM,
		CAST(PTYP_ID AS VARCHAR(MAX)) AS PTYP_ID,
		CAST(PTYP_ID_TEXT AS VARCHAR(MAX)) AS PTYP_ID_TEXT,
		CAST(PTYP_PASSIV AS VARCHAR(MAX)) AS PTYP_PASSIV,
		CAST(PTYP_TEXT AS VARCHAR(MAX)) AS PTYP_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    
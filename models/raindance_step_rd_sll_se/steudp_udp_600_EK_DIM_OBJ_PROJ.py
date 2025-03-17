
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
		CONVERT(varchar(max), BE_GILTIG_FOM, 126) AS be_giltig_fom,
		CONVERT(varchar(max), BE_GILTIG_TOM, 126) AS be_giltig_tom,
		CAST(BE_ID AS VARCHAR(MAX)) AS be_id,
		CAST(BE_ID_TEXT AS VARCHAR(MAX)) AS be_id_text,
		CAST(BE_PASSIV AS VARCHAR(MAX)) AS be_passiv,
		CAST(BE_TEXT AS VARCHAR(MAX)) AS be_text,
		CONVERT(varchar(max), PANS_GILTIG_FOM, 126) AS pans_giltig_fom,
		CONVERT(varchar(max), PANS_GILTIG_TOM, 126) AS pans_giltig_tom,
		CAST(PANS_ID AS VARCHAR(MAX)) AS pans_id,
		CAST(PANS_ID_TEXT AS VARCHAR(MAX)) AS pans_id_text,
		CAST(PANS_PASSIV AS VARCHAR(MAX)) AS pans_passiv,
		CAST(PANS_TEXT AS VARCHAR(MAX)) AS pans_text,
		CONVERT(varchar(max), PKAT_GILTIG_FOM, 126) AS pkat_giltig_fom,
		CONVERT(varchar(max), PKAT_GILTIG_TOM, 126) AS pkat_giltig_tom,
		CAST(PKAT_ID AS VARCHAR(MAX)) AS pkat_id,
		CAST(PKAT_ID_TEXT AS VARCHAR(MAX)) AS pkat_id_text,
		CAST(PKAT_PASSIV AS VARCHAR(MAX)) AS pkat_passiv,
		CAST(PKAT_TEXT AS VARCHAR(MAX)) AS pkat_text,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS proj_giltig_fom,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS proj_giltig_tom,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS proj_id_text,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS proj_passiv,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS proj_text,
		CONVERT(varchar(max), PTYP_GILTIG_FOM, 126) AS ptyp_giltig_fom,
		CONVERT(varchar(max), PTYP_GILTIG_TOM, 126) AS ptyp_giltig_tom,
		CAST(PTYP_ID AS VARCHAR(MAX)) AS ptyp_id,
		CAST(PTYP_ID_TEXT AS VARCHAR(MAX)) AS ptyp_id_text,
		CAST(PTYP_PASSIV AS VARCHAR(MAX)) AS ptyp_passiv,
		CAST(PTYP_TEXT AS VARCHAR(MAX)) AS ptyp_text 
	FROM steudp.udp_600.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    
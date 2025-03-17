
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAKINT_GILTIG_FOM': 'varchar(max)', 'FAKINT_GILTIG_TOM': 'varchar(max)', 'FAKINT_ID': 'varchar(max)', 'FAKINT_ID_TEXT': 'varchar(max)', 'FAKINT_PASSIV': 'varchar(max)', 'FAKINT_TEXT': 'varchar(max)', 'PANM_GILTIG_FOM': 'varchar(max)', 'PANM_GILTIG_TOM': 'varchar(max)', 'PANM_ID': 'varchar(max)', 'PANM_ID_TEXT': 'varchar(max)', 'PANM_PASSIV': 'varchar(max)', 'PANM_TEXT': 'varchar(max)', 'PDIARI_GILTIG_FOM': 'varchar(max)', 'PDIARI_GILTIG_TOM': 'varchar(max)', 'PDIARI_ID': 'varchar(max)', 'PDIARI_ID_TEXT': 'varchar(max)', 'PDIARI_PASSIV': 'varchar(max)', 'PDIARI_TEXT': 'varchar(max)', 'PHSAID_GILTIG_FOM': 'varchar(max)', 'PHSAID_GILTIG_TOM': 'varchar(max)', 'PHSAID_ID': 'varchar(max)', 'PHSAID_ID_TEXT': 'varchar(max)', 'PHSAID_PASSIV': 'varchar(max)', 'PHSAID_TEXT': 'varchar(max)', 'PKLINI_GILTIG_FOM': 'varchar(max)', 'PKLINI_GILTIG_TOM': 'varchar(max)', 'PKLINI_ID': 'varchar(max)', 'PKLINI_ID_TEXT': 'varchar(max)', 'PKLINI_PASSIV': 'varchar(max)', 'PKLINI_TEXT': 'varchar(max)', 'PLED_GILTIG_FOM': 'varchar(max)', 'PLED_GILTIG_TOM': 'varchar(max)', 'PLED_ID': 'varchar(max)', 'PLED_ID_TEXT': 'varchar(max)', 'PLED_PASSIV': 'varchar(max)', 'PLED_TEXT': 'varchar(max)', 'POH_GILTIG_FOM': 'varchar(max)', 'POH_GILTIG_TOM': 'varchar(max)', 'POH_ID': 'varchar(max)', 'POH_ID_TEXT': 'varchar(max)', 'POH_PASSIV': 'varchar(max)', 'POH_TEXT': 'varchar(max)', 'PORGNR_GILTIG_FOM': 'varchar(max)', 'PORGNR_GILTIG_TOM': 'varchar(max)', 'PORGNR_ID': 'varchar(max)', 'PORGNR_ID_TEXT': 'varchar(max)', 'PORGNR_PASSIV': 'varchar(max)', 'PORGNR_TEXT': 'varchar(max)', 'PROHUV_GILTIG_FOM': 'varchar(max)', 'PROHUV_GILTIG_TOM': 'varchar(max)', 'PROHUV_ID': 'varchar(max)', 'PROHUV_ID_TEXT': 'varchar(max)', 'PROHUV_PASSIV': 'varchar(max)', 'PROHUV_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)', 'PROSTA_GILTIG_FOM': 'varchar(max)', 'PROSTA_GILTIG_TOM': 'varchar(max)', 'PROSTA_ID': 'varchar(max)', 'PROSTA_ID_TEXT': 'varchar(max)', 'PROSTA_PASSIV': 'varchar(max)', 'PROSTA_TEXT': 'varchar(max)', 'PTYP_GILTIG_FOM': 'varchar(max)', 'PTYP_GILTIG_TOM': 'varchar(max)', 'PTYP_ID': 'varchar(max)', 'PTYP_ID_TEXT': 'varchar(max)', 'PTYP_PASSIV': 'varchar(max)', 'PTYP_TEXT': 'varchar(max)', 'PÅTBET_GILTIG_FOM': 'varchar(max)', 'PÅTBET_GILTIG_TOM': 'varchar(max)', 'PÅTBET_ID': 'varchar(max)', 'PÅTBET_ID_TEXT': 'varchar(max)', 'PÅTBET_PASSIV': 'varchar(max)', 'PÅTBET_TEXT': 'varchar(max)', 'PÅTRAP_GILTIG_FOM': 'varchar(max)', 'PÅTRAP_GILTIG_TOM': 'varchar(max)', 'PÅTRAP_ID': 'varchar(max)', 'PÅTRAP_ID_TEXT': 'varchar(max)', 'PÅTRAP_PASSIV': 'varchar(max)', 'PÅTRAP_TEXT': 'varchar(max)', 'XPLED_GILTIG_FOM': 'varchar(max)', 'XPLED_GILTIG_TOM': 'varchar(max)', 'XPLED_ID': 'varchar(max)', 'XPLED_ID_TEXT': 'varchar(max)', 'XPLED_PASSIV': 'varchar(max)', 'XPLED_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), FAKINT_GILTIG_FOM, 126) AS fakint_giltig_fom,
		CONVERT(varchar(max), FAKINT_GILTIG_TOM, 126) AS fakint_giltig_tom,
		CAST(FAKINT_ID AS VARCHAR(MAX)) AS fakint_id,
		CAST(FAKINT_ID_TEXT AS VARCHAR(MAX)) AS fakint_id_text,
		CAST(FAKINT_PASSIV AS VARCHAR(MAX)) AS fakint_passiv,
		CAST(FAKINT_TEXT AS VARCHAR(MAX)) AS fakint_text,
		CONVERT(varchar(max), PANM_GILTIG_FOM, 126) AS panm_giltig_fom,
		CONVERT(varchar(max), PANM_GILTIG_TOM, 126) AS panm_giltig_tom,
		CAST(PANM_ID AS VARCHAR(MAX)) AS panm_id,
		CAST(PANM_ID_TEXT AS VARCHAR(MAX)) AS panm_id_text,
		CAST(PANM_PASSIV AS VARCHAR(MAX)) AS panm_passiv,
		CAST(PANM_TEXT AS VARCHAR(MAX)) AS panm_text,
		CONVERT(varchar(max), PDIARI_GILTIG_FOM, 126) AS pdiari_giltig_fom,
		CONVERT(varchar(max), PDIARI_GILTIG_TOM, 126) AS pdiari_giltig_tom,
		CAST(PDIARI_ID AS VARCHAR(MAX)) AS pdiari_id,
		CAST(PDIARI_ID_TEXT AS VARCHAR(MAX)) AS pdiari_id_text,
		CAST(PDIARI_PASSIV AS VARCHAR(MAX)) AS pdiari_passiv,
		CAST(PDIARI_TEXT AS VARCHAR(MAX)) AS pdiari_text,
		CONVERT(varchar(max), PHSAID_GILTIG_FOM, 126) AS phsaid_giltig_fom,
		CONVERT(varchar(max), PHSAID_GILTIG_TOM, 126) AS phsaid_giltig_tom,
		CAST(PHSAID_ID AS VARCHAR(MAX)) AS phsaid_id,
		CAST(PHSAID_ID_TEXT AS VARCHAR(MAX)) AS phsaid_id_text,
		CAST(PHSAID_PASSIV AS VARCHAR(MAX)) AS phsaid_passiv,
		CAST(PHSAID_TEXT AS VARCHAR(MAX)) AS phsaid_text,
		CONVERT(varchar(max), PKLINI_GILTIG_FOM, 126) AS pklini_giltig_fom,
		CONVERT(varchar(max), PKLINI_GILTIG_TOM, 126) AS pklini_giltig_tom,
		CAST(PKLINI_ID AS VARCHAR(MAX)) AS pklini_id,
		CAST(PKLINI_ID_TEXT AS VARCHAR(MAX)) AS pklini_id_text,
		CAST(PKLINI_PASSIV AS VARCHAR(MAX)) AS pklini_passiv,
		CAST(PKLINI_TEXT AS VARCHAR(MAX)) AS pklini_text,
		CONVERT(varchar(max), PLED_GILTIG_FOM, 126) AS pled_giltig_fom,
		CONVERT(varchar(max), PLED_GILTIG_TOM, 126) AS pled_giltig_tom,
		CAST(PLED_ID AS VARCHAR(MAX)) AS pled_id,
		CAST(PLED_ID_TEXT AS VARCHAR(MAX)) AS pled_id_text,
		CAST(PLED_PASSIV AS VARCHAR(MAX)) AS pled_passiv,
		CAST(PLED_TEXT AS VARCHAR(MAX)) AS pled_text,
		CONVERT(varchar(max), POH_GILTIG_FOM, 126) AS poh_giltig_fom,
		CONVERT(varchar(max), POH_GILTIG_TOM, 126) AS poh_giltig_tom,
		CAST(POH_ID AS VARCHAR(MAX)) AS poh_id,
		CAST(POH_ID_TEXT AS VARCHAR(MAX)) AS poh_id_text,
		CAST(POH_PASSIV AS VARCHAR(MAX)) AS poh_passiv,
		CAST(POH_TEXT AS VARCHAR(MAX)) AS poh_text,
		CONVERT(varchar(max), PORGNR_GILTIG_FOM, 126) AS porgnr_giltig_fom,
		CONVERT(varchar(max), PORGNR_GILTIG_TOM, 126) AS porgnr_giltig_tom,
		CAST(PORGNR_ID AS VARCHAR(MAX)) AS porgnr_id,
		CAST(PORGNR_ID_TEXT AS VARCHAR(MAX)) AS porgnr_id_text,
		CAST(PORGNR_PASSIV AS VARCHAR(MAX)) AS porgnr_passiv,
		CAST(PORGNR_TEXT AS VARCHAR(MAX)) AS porgnr_text,
		CONVERT(varchar(max), PROHUV_GILTIG_FOM, 126) AS prohuv_giltig_fom,
		CONVERT(varchar(max), PROHUV_GILTIG_TOM, 126) AS prohuv_giltig_tom,
		CAST(PROHUV_ID AS VARCHAR(MAX)) AS prohuv_id,
		CAST(PROHUV_ID_TEXT AS VARCHAR(MAX)) AS prohuv_id_text,
		CAST(PROHUV_PASSIV AS VARCHAR(MAX)) AS prohuv_passiv,
		CAST(PROHUV_TEXT AS VARCHAR(MAX)) AS prohuv_text,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS proj_giltig_fom,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS proj_giltig_tom,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS proj_id_text,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS proj_passiv,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS proj_text,
		CONVERT(varchar(max), PROSTA_GILTIG_FOM, 126) AS prosta_giltig_fom,
		CONVERT(varchar(max), PROSTA_GILTIG_TOM, 126) AS prosta_giltig_tom,
		CAST(PROSTA_ID AS VARCHAR(MAX)) AS prosta_id,
		CAST(PROSTA_ID_TEXT AS VARCHAR(MAX)) AS prosta_id_text,
		CAST(PROSTA_PASSIV AS VARCHAR(MAX)) AS prosta_passiv,
		CAST(PROSTA_TEXT AS VARCHAR(MAX)) AS prosta_text,
		CONVERT(varchar(max), PTYP_GILTIG_FOM, 126) AS ptyp_giltig_fom,
		CONVERT(varchar(max), PTYP_GILTIG_TOM, 126) AS ptyp_giltig_tom,
		CAST(PTYP_ID AS VARCHAR(MAX)) AS ptyp_id,
		CAST(PTYP_ID_TEXT AS VARCHAR(MAX)) AS ptyp_id_text,
		CAST(PTYP_PASSIV AS VARCHAR(MAX)) AS ptyp_passiv,
		CAST(PTYP_TEXT AS VARCHAR(MAX)) AS ptyp_text,
		CONVERT(varchar(max), PÅTBET_GILTIG_FOM, 126) AS påtbet_giltig_fom,
		CONVERT(varchar(max), PÅTBET_GILTIG_TOM, 126) AS påtbet_giltig_tom,
		CAST(PÅTBET_ID AS VARCHAR(MAX)) AS påtbet_id,
		CAST(PÅTBET_ID_TEXT AS VARCHAR(MAX)) AS påtbet_id_text,
		CAST(PÅTBET_PASSIV AS VARCHAR(MAX)) AS påtbet_passiv,
		CAST(PÅTBET_TEXT AS VARCHAR(MAX)) AS påtbet_text,
		CONVERT(varchar(max), PÅTRAP_GILTIG_FOM, 126) AS påtrap_giltig_fom,
		CONVERT(varchar(max), PÅTRAP_GILTIG_TOM, 126) AS påtrap_giltig_tom,
		CAST(PÅTRAP_ID AS VARCHAR(MAX)) AS påtrap_id,
		CAST(PÅTRAP_ID_TEXT AS VARCHAR(MAX)) AS påtrap_id_text,
		CAST(PÅTRAP_PASSIV AS VARCHAR(MAX)) AS påtrap_passiv,
		CAST(PÅTRAP_TEXT AS VARCHAR(MAX)) AS påtrap_text,
		CONVERT(varchar(max), XPLED_GILTIG_FOM, 126) AS xpled_giltig_fom,
		CONVERT(varchar(max), XPLED_GILTIG_TOM, 126) AS xpled_giltig_tom,
		CAST(XPLED_ID AS VARCHAR(MAX)) AS xpled_id,
		CAST(XPLED_ID_TEXT AS VARCHAR(MAX)) AS xpled_id_text,
		CAST(XPLED_PASSIV AS VARCHAR(MAX)) AS xpled_passiv,
		CAST(XPLED_TEXT AS VARCHAR(MAX)) AS xpled_text 
	FROM Utdata.udp_100.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    
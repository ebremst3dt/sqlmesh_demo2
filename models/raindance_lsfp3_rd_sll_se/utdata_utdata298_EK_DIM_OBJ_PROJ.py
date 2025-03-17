
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVSLÅR_GILTIG_FOM': 'varchar(max)', 'AVSLÅR_GILTIG_TOM': 'varchar(max)', 'AVSLÅR_ID': 'varchar(max)', 'AVSLÅR_ID_TEXT': 'varchar(max)', 'AVSLÅR_PASSIV': 'varchar(max)', 'AVSLÅR_TEXT': 'varchar(max)', 'EKTYP_GILTIG_FOM': 'varchar(max)', 'EKTYP_GILTIG_TOM': 'varchar(max)', 'EKTYP_ID': 'varchar(max)', 'EKTYP_ID_TEXT': 'varchar(max)', 'EKTYP_PASSIV': 'varchar(max)', 'EKTYP_TEXT': 'varchar(max)', 'FIFORM_GILTIG_FOM': 'varchar(max)', 'FIFORM_GILTIG_TOM': 'varchar(max)', 'FIFORM_ID': 'varchar(max)', 'FIFORM_ID_TEXT': 'varchar(max)', 'FIFORM_PASSIV': 'varchar(max)', 'FIFORM_TEXT': 'varchar(max)', 'LEVANS_GILTIG_FOM': 'varchar(max)', 'LEVANS_GILTIG_TOM': 'varchar(max)', 'LEVANS_ID': 'varchar(max)', 'LEVANS_ID_TEXT': 'varchar(max)', 'LEVANS_PASSIV': 'varchar(max)', 'LEVANS_TEXT': 'varchar(max)', 'PRADM_GILTIG_FOM': 'varchar(max)', 'PRADM_GILTIG_TOM': 'varchar(max)', 'PRADM_ID': 'varchar(max)', 'PRADM_ID_TEXT': 'varchar(max)', 'PRADM_PASSIV': 'varchar(max)', 'PRADM_TEXT': 'varchar(max)', 'PRJINR_GILTIG_FOM': 'varchar(max)', 'PRJINR_GILTIG_TOM': 'varchar(max)', 'PRJINR_ID': 'varchar(max)', 'PRJINR_ID_TEXT': 'varchar(max)', 'PRJINR_PASSIV': 'varchar(max)', 'PRJINR_TEXT': 'varchar(max)', 'PROENH_GILTIG_FOM': 'varchar(max)', 'PROENH_GILTIG_TOM': 'varchar(max)', 'PROENH_ID': 'varchar(max)', 'PROENH_ID_TEXT': 'varchar(max)', 'PROENH_PASSIV': 'varchar(max)', 'PROENH_TEXT': 'varchar(max)', 'PROJL_GILTIG_FOM': 'varchar(max)', 'PROJL_GILTIG_TOM': 'varchar(max)', 'PROJL_ID': 'varchar(max)', 'PROJL_ID_TEXT': 'varchar(max)', 'PROJL_PASSIV': 'varchar(max)', 'PROJL_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), AVSLÅR_GILTIG_FOM, 126) AS avslår_giltig_fom,
		CONVERT(varchar(max), AVSLÅR_GILTIG_TOM, 126) AS avslår_giltig_tom,
		CAST(AVSLÅR_ID AS VARCHAR(MAX)) AS avslår_id,
		CAST(AVSLÅR_ID_TEXT AS VARCHAR(MAX)) AS avslår_id_text,
		CAST(AVSLÅR_PASSIV AS VARCHAR(MAX)) AS avslår_passiv,
		CAST(AVSLÅR_TEXT AS VARCHAR(MAX)) AS avslår_text,
		CONVERT(varchar(max), EKTYP_GILTIG_FOM, 126) AS ektyp_giltig_fom,
		CONVERT(varchar(max), EKTYP_GILTIG_TOM, 126) AS ektyp_giltig_tom,
		CAST(EKTYP_ID AS VARCHAR(MAX)) AS ektyp_id,
		CAST(EKTYP_ID_TEXT AS VARCHAR(MAX)) AS ektyp_id_text,
		CAST(EKTYP_PASSIV AS VARCHAR(MAX)) AS ektyp_passiv,
		CAST(EKTYP_TEXT AS VARCHAR(MAX)) AS ektyp_text,
		CONVERT(varchar(max), FIFORM_GILTIG_FOM, 126) AS fiform_giltig_fom,
		CONVERT(varchar(max), FIFORM_GILTIG_TOM, 126) AS fiform_giltig_tom,
		CAST(FIFORM_ID AS VARCHAR(MAX)) AS fiform_id,
		CAST(FIFORM_ID_TEXT AS VARCHAR(MAX)) AS fiform_id_text,
		CAST(FIFORM_PASSIV AS VARCHAR(MAX)) AS fiform_passiv,
		CAST(FIFORM_TEXT AS VARCHAR(MAX)) AS fiform_text,
		CONVERT(varchar(max), LEVANS_GILTIG_FOM, 126) AS levans_giltig_fom,
		CONVERT(varchar(max), LEVANS_GILTIG_TOM, 126) AS levans_giltig_tom,
		CAST(LEVANS_ID AS VARCHAR(MAX)) AS levans_id,
		CAST(LEVANS_ID_TEXT AS VARCHAR(MAX)) AS levans_id_text,
		CAST(LEVANS_PASSIV AS VARCHAR(MAX)) AS levans_passiv,
		CAST(LEVANS_TEXT AS VARCHAR(MAX)) AS levans_text,
		CONVERT(varchar(max), PRADM_GILTIG_FOM, 126) AS pradm_giltig_fom,
		CONVERT(varchar(max), PRADM_GILTIG_TOM, 126) AS pradm_giltig_tom,
		CAST(PRADM_ID AS VARCHAR(MAX)) AS pradm_id,
		CAST(PRADM_ID_TEXT AS VARCHAR(MAX)) AS pradm_id_text,
		CAST(PRADM_PASSIV AS VARCHAR(MAX)) AS pradm_passiv,
		CAST(PRADM_TEXT AS VARCHAR(MAX)) AS pradm_text,
		CONVERT(varchar(max), PRJINR_GILTIG_FOM, 126) AS prjinr_giltig_fom,
		CONVERT(varchar(max), PRJINR_GILTIG_TOM, 126) AS prjinr_giltig_tom,
		CAST(PRJINR_ID AS VARCHAR(MAX)) AS prjinr_id,
		CAST(PRJINR_ID_TEXT AS VARCHAR(MAX)) AS prjinr_id_text,
		CAST(PRJINR_PASSIV AS VARCHAR(MAX)) AS prjinr_passiv,
		CAST(PRJINR_TEXT AS VARCHAR(MAX)) AS prjinr_text,
		CONVERT(varchar(max), PROENH_GILTIG_FOM, 126) AS proenh_giltig_fom,
		CONVERT(varchar(max), PROENH_GILTIG_TOM, 126) AS proenh_giltig_tom,
		CAST(PROENH_ID AS VARCHAR(MAX)) AS proenh_id,
		CAST(PROENH_ID_TEXT AS VARCHAR(MAX)) AS proenh_id_text,
		CAST(PROENH_PASSIV AS VARCHAR(MAX)) AS proenh_passiv,
		CAST(PROENH_TEXT AS VARCHAR(MAX)) AS proenh_text,
		CONVERT(varchar(max), PROJL_GILTIG_FOM, 126) AS projl_giltig_fom,
		CONVERT(varchar(max), PROJL_GILTIG_TOM, 126) AS projl_giltig_tom,
		CAST(PROJL_ID AS VARCHAR(MAX)) AS projl_id,
		CAST(PROJL_ID_TEXT AS VARCHAR(MAX)) AS projl_id_text,
		CAST(PROJL_PASSIV AS VARCHAR(MAX)) AS projl_passiv,
		CAST(PROJL_TEXT AS VARCHAR(MAX)) AS projl_text,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS proj_giltig_fom,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS proj_giltig_tom,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS proj_id_text,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS proj_passiv,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS proj_text 
	FROM utdata.utdata298.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
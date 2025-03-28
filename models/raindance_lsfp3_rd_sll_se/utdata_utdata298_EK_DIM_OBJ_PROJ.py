
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
		CONVERT(varchar(max), AVSLÅR_GILTIG_FOM, 126) AS AVSLÅR_GILTIG_FOM,
		CONVERT(varchar(max), AVSLÅR_GILTIG_TOM, 126) AS AVSLÅR_GILTIG_TOM,
		CAST(AVSLÅR_ID AS VARCHAR(MAX)) AS AVSLÅR_ID,
		CAST(AVSLÅR_ID_TEXT AS VARCHAR(MAX)) AS AVSLÅR_ID_TEXT,
		CAST(AVSLÅR_PASSIV AS VARCHAR(MAX)) AS AVSLÅR_PASSIV,
		CAST(AVSLÅR_TEXT AS VARCHAR(MAX)) AS AVSLÅR_TEXT,
		CONVERT(varchar(max), EKTYP_GILTIG_FOM, 126) AS EKTYP_GILTIG_FOM,
		CONVERT(varchar(max), EKTYP_GILTIG_TOM, 126) AS EKTYP_GILTIG_TOM,
		CAST(EKTYP_ID AS VARCHAR(MAX)) AS EKTYP_ID,
		CAST(EKTYP_ID_TEXT AS VARCHAR(MAX)) AS EKTYP_ID_TEXT,
		CAST(EKTYP_PASSIV AS VARCHAR(MAX)) AS EKTYP_PASSIV,
		CAST(EKTYP_TEXT AS VARCHAR(MAX)) AS EKTYP_TEXT,
		CONVERT(varchar(max), FIFORM_GILTIG_FOM, 126) AS FIFORM_GILTIG_FOM,
		CONVERT(varchar(max), FIFORM_GILTIG_TOM, 126) AS FIFORM_GILTIG_TOM,
		CAST(FIFORM_ID AS VARCHAR(MAX)) AS FIFORM_ID,
		CAST(FIFORM_ID_TEXT AS VARCHAR(MAX)) AS FIFORM_ID_TEXT,
		CAST(FIFORM_PASSIV AS VARCHAR(MAX)) AS FIFORM_PASSIV,
		CAST(FIFORM_TEXT AS VARCHAR(MAX)) AS FIFORM_TEXT,
		CONVERT(varchar(max), LEVANS_GILTIG_FOM, 126) AS LEVANS_GILTIG_FOM,
		CONVERT(varchar(max), LEVANS_GILTIG_TOM, 126) AS LEVANS_GILTIG_TOM,
		CAST(LEVANS_ID AS VARCHAR(MAX)) AS LEVANS_ID,
		CAST(LEVANS_ID_TEXT AS VARCHAR(MAX)) AS LEVANS_ID_TEXT,
		CAST(LEVANS_PASSIV AS VARCHAR(MAX)) AS LEVANS_PASSIV,
		CAST(LEVANS_TEXT AS VARCHAR(MAX)) AS LEVANS_TEXT,
		CONVERT(varchar(max), PRADM_GILTIG_FOM, 126) AS PRADM_GILTIG_FOM,
		CONVERT(varchar(max), PRADM_GILTIG_TOM, 126) AS PRADM_GILTIG_TOM,
		CAST(PRADM_ID AS VARCHAR(MAX)) AS PRADM_ID,
		CAST(PRADM_ID_TEXT AS VARCHAR(MAX)) AS PRADM_ID_TEXT,
		CAST(PRADM_PASSIV AS VARCHAR(MAX)) AS PRADM_PASSIV,
		CAST(PRADM_TEXT AS VARCHAR(MAX)) AS PRADM_TEXT,
		CONVERT(varchar(max), PRJINR_GILTIG_FOM, 126) AS PRJINR_GILTIG_FOM,
		CONVERT(varchar(max), PRJINR_GILTIG_TOM, 126) AS PRJINR_GILTIG_TOM,
		CAST(PRJINR_ID AS VARCHAR(MAX)) AS PRJINR_ID,
		CAST(PRJINR_ID_TEXT AS VARCHAR(MAX)) AS PRJINR_ID_TEXT,
		CAST(PRJINR_PASSIV AS VARCHAR(MAX)) AS PRJINR_PASSIV,
		CAST(PRJINR_TEXT AS VARCHAR(MAX)) AS PRJINR_TEXT,
		CONVERT(varchar(max), PROENH_GILTIG_FOM, 126) AS PROENH_GILTIG_FOM,
		CONVERT(varchar(max), PROENH_GILTIG_TOM, 126) AS PROENH_GILTIG_TOM,
		CAST(PROENH_ID AS VARCHAR(MAX)) AS PROENH_ID,
		CAST(PROENH_ID_TEXT AS VARCHAR(MAX)) AS PROENH_ID_TEXT,
		CAST(PROENH_PASSIV AS VARCHAR(MAX)) AS PROENH_PASSIV,
		CAST(PROENH_TEXT AS VARCHAR(MAX)) AS PROENH_TEXT,
		CONVERT(varchar(max), PROJL_GILTIG_FOM, 126) AS PROJL_GILTIG_FOM,
		CONVERT(varchar(max), PROJL_GILTIG_TOM, 126) AS PROJL_GILTIG_TOM,
		CAST(PROJL_ID AS VARCHAR(MAX)) AS PROJL_ID,
		CAST(PROJL_ID_TEXT AS VARCHAR(MAX)) AS PROJL_ID_TEXT,
		CAST(PROJL_PASSIV AS VARCHAR(MAX)) AS PROJL_PASSIV,
		CAST(PROJL_TEXT AS VARCHAR(MAX)) AS PROJL_TEXT,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS PROJ_GILTIG_FOM,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS PROJ_GILTIG_TOM,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS PROJ_ID_TEXT,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS PROJ_PASSIV,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS PROJ_TEXT 
	FROM utdata.utdata298.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    
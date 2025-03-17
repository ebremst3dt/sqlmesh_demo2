
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'FRARAD_GILTIG_FOM': 'varchar(max)', 'FRARAD_GILTIG_TOM': 'varchar(max)', 'FRARAD_ID': 'varchar(max)', 'FRARAD_ID_TEXT': 'varchar(max)', 'FRARAD_PASSIV': 'varchar(max)', 'FRARAD_TEXT': 'varchar(max)', 'FRASUM_GILTIG_FOM': 'varchar(max)', 'FRASUM_GILTIG_TOM': 'varchar(max)', 'FRASUM_ID': 'varchar(max)', 'FRASUM_ID_TEXT': 'varchar(max)', 'FRASUM_PASSIV': 'varchar(max)', 'FRASUM_TEXT': 'varchar(max)', 'FRIRA2_GILTIG_FOM': 'varchar(max)', 'FRIRA2_GILTIG_TOM': 'varchar(max)', 'FRIRA2_ID': 'varchar(max)', 'FRIRA2_ID_TEXT': 'varchar(max)', 'FRIRA2_PASSIV': 'varchar(max)', 'FRIRA2_TEXT': 'varchar(max)', 'FRIRAD_GILTIG_FOM': 'varchar(max)', 'FRIRAD_GILTIG_TOM': 'varchar(max)', 'FRIRAD_ID': 'varchar(max)', 'FRIRAD_ID_TEXT': 'varchar(max)', 'FRIRAD_PASSIV': 'varchar(max)', 'FRIRAD_TEXT': 'varchar(max)', 'FRISUM_GILTIG_FOM': 'varchar(max)', 'FRISUM_GILTIG_TOM': 'varchar(max)', 'FRISUM_ID': 'varchar(max)', 'FRISUM_ID_TEXT': 'varchar(max)', 'FRISUM_PASSIV': 'varchar(max)', 'FRISUM_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'KRES_GILTIG_FOM': 'varchar(max)', 'KRES_GILTIG_TOM': 'varchar(max)', 'KRES_ID': 'varchar(max)', 'KRES_ID_TEXT': 'varchar(max)', 'KRES_PASSIV': 'varchar(max)', 'KRES_TEXT': 'varchar(max)', 'KRGRP_GILTIG_FOM': 'varchar(max)', 'KRGRP_GILTIG_TOM': 'varchar(max)', 'KRGRP_ID': 'varchar(max)', 'KRGRP_ID_TEXT': 'varchar(max)', 'KRGRP_PASSIV': 'varchar(max)', 'KRGRP_TEXT': 'varchar(max)', 'KTO2_GILTIG_FOM': 'varchar(max)', 'KTO2_GILTIG_TOM': 'varchar(max)', 'KTO2_ID': 'varchar(max)', 'KTO2_ID_TEXT': 'varchar(max)', 'KTO2_PASSIV': 'varchar(max)', 'KTO2_TEXT': 'varchar(max)', 'SJKGRP_GILTIG_FOM': 'varchar(max)', 'SJKGRP_GILTIG_TOM': 'varchar(max)', 'SJKGRP_ID': 'varchar(max)', 'SJKGRP_ID_TEXT': 'varchar(max)', 'SJKGRP_PASSIV': 'varchar(max)', 'SJKGRP_TEXT': 'varchar(max)', 'SJKRAD_GILTIG_FOM': 'varchar(max)', 'SJKRAD_GILTIG_TOM': 'varchar(max)', 'SJKRAD_ID': 'varchar(max)', 'SJKRAD_ID_TEXT': 'varchar(max)', 'SJKRAD_PASSIV': 'varchar(max)', 'SJKRAD_TEXT': 'varchar(max)', 'UKONTO_GILTIG_FOM': 'varchar(max)', 'UKONTO_GILTIG_TOM': 'varchar(max)', 'UKONTO_ID': 'varchar(max)', 'UKONTO_ID_TEXT': 'varchar(max)', 'UKONTO_PASSIV': 'varchar(max)', 'UKONTO_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), FRARAD_GILTIG_FOM, 126) AS frarad_giltig_fom,
		CONVERT(varchar(max), FRARAD_GILTIG_TOM, 126) AS frarad_giltig_tom,
		CAST(FRARAD_ID AS VARCHAR(MAX)) AS frarad_id,
		CAST(FRARAD_ID_TEXT AS VARCHAR(MAX)) AS frarad_id_text,
		CAST(FRARAD_PASSIV AS VARCHAR(MAX)) AS frarad_passiv,
		CAST(FRARAD_TEXT AS VARCHAR(MAX)) AS frarad_text,
		CONVERT(varchar(max), FRASUM_GILTIG_FOM, 126) AS frasum_giltig_fom,
		CONVERT(varchar(max), FRASUM_GILTIG_TOM, 126) AS frasum_giltig_tom,
		CAST(FRASUM_ID AS VARCHAR(MAX)) AS frasum_id,
		CAST(FRASUM_ID_TEXT AS VARCHAR(MAX)) AS frasum_id_text,
		CAST(FRASUM_PASSIV AS VARCHAR(MAX)) AS frasum_passiv,
		CAST(FRASUM_TEXT AS VARCHAR(MAX)) AS frasum_text,
		CONVERT(varchar(max), FRIRA2_GILTIG_FOM, 126) AS frira2_giltig_fom,
		CONVERT(varchar(max), FRIRA2_GILTIG_TOM, 126) AS frira2_giltig_tom,
		CAST(FRIRA2_ID AS VARCHAR(MAX)) AS frira2_id,
		CAST(FRIRA2_ID_TEXT AS VARCHAR(MAX)) AS frira2_id_text,
		CAST(FRIRA2_PASSIV AS VARCHAR(MAX)) AS frira2_passiv,
		CAST(FRIRA2_TEXT AS VARCHAR(MAX)) AS frira2_text,
		CONVERT(varchar(max), FRIRAD_GILTIG_FOM, 126) AS frirad_giltig_fom,
		CONVERT(varchar(max), FRIRAD_GILTIG_TOM, 126) AS frirad_giltig_tom,
		CAST(FRIRAD_ID AS VARCHAR(MAX)) AS frirad_id,
		CAST(FRIRAD_ID_TEXT AS VARCHAR(MAX)) AS frirad_id_text,
		CAST(FRIRAD_PASSIV AS VARCHAR(MAX)) AS frirad_passiv,
		CAST(FRIRAD_TEXT AS VARCHAR(MAX)) AS frirad_text,
		CONVERT(varchar(max), FRISUM_GILTIG_FOM, 126) AS frisum_giltig_fom,
		CONVERT(varchar(max), FRISUM_GILTIG_TOM, 126) AS frisum_giltig_tom,
		CAST(FRISUM_ID AS VARCHAR(MAX)) AS frisum_id,
		CAST(FRISUM_ID_TEXT AS VARCHAR(MAX)) AS frisum_id_text,
		CAST(FRISUM_PASSIV AS VARCHAR(MAX)) AS frisum_passiv,
		CAST(FRISUM_TEXT AS VARCHAR(MAX)) AS frisum_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), KRES_GILTIG_FOM, 126) AS kres_giltig_fom,
		CONVERT(varchar(max), KRES_GILTIG_TOM, 126) AS kres_giltig_tom,
		CAST(KRES_ID AS VARCHAR(MAX)) AS kres_id,
		CAST(KRES_ID_TEXT AS VARCHAR(MAX)) AS kres_id_text,
		CAST(KRES_PASSIV AS VARCHAR(MAX)) AS kres_passiv,
		CAST(KRES_TEXT AS VARCHAR(MAX)) AS kres_text,
		CONVERT(varchar(max), KRGRP_GILTIG_FOM, 126) AS krgrp_giltig_fom,
		CONVERT(varchar(max), KRGRP_GILTIG_TOM, 126) AS krgrp_giltig_tom,
		CAST(KRGRP_ID AS VARCHAR(MAX)) AS krgrp_id,
		CAST(KRGRP_ID_TEXT AS VARCHAR(MAX)) AS krgrp_id_text,
		CAST(KRGRP_PASSIV AS VARCHAR(MAX)) AS krgrp_passiv,
		CAST(KRGRP_TEXT AS VARCHAR(MAX)) AS krgrp_text,
		CONVERT(varchar(max), KTO2_GILTIG_FOM, 126) AS kto2_giltig_fom,
		CONVERT(varchar(max), KTO2_GILTIG_TOM, 126) AS kto2_giltig_tom,
		CAST(KTO2_ID AS VARCHAR(MAX)) AS kto2_id,
		CAST(KTO2_ID_TEXT AS VARCHAR(MAX)) AS kto2_id_text,
		CAST(KTO2_PASSIV AS VARCHAR(MAX)) AS kto2_passiv,
		CAST(KTO2_TEXT AS VARCHAR(MAX)) AS kto2_text,
		CONVERT(varchar(max), SJKGRP_GILTIG_FOM, 126) AS sjkgrp_giltig_fom,
		CONVERT(varchar(max), SJKGRP_GILTIG_TOM, 126) AS sjkgrp_giltig_tom,
		CAST(SJKGRP_ID AS VARCHAR(MAX)) AS sjkgrp_id,
		CAST(SJKGRP_ID_TEXT AS VARCHAR(MAX)) AS sjkgrp_id_text,
		CAST(SJKGRP_PASSIV AS VARCHAR(MAX)) AS sjkgrp_passiv,
		CAST(SJKGRP_TEXT AS VARCHAR(MAX)) AS sjkgrp_text,
		CONVERT(varchar(max), SJKRAD_GILTIG_FOM, 126) AS sjkrad_giltig_fom,
		CONVERT(varchar(max), SJKRAD_GILTIG_TOM, 126) AS sjkrad_giltig_tom,
		CAST(SJKRAD_ID AS VARCHAR(MAX)) AS sjkrad_id,
		CAST(SJKRAD_ID_TEXT AS VARCHAR(MAX)) AS sjkrad_id_text,
		CAST(SJKRAD_PASSIV AS VARCHAR(MAX)) AS sjkrad_passiv,
		CAST(SJKRAD_TEXT AS VARCHAR(MAX)) AS sjkrad_text,
		CONVERT(varchar(max), UKONTO_GILTIG_FOM, 126) AS ukonto_giltig_fom,
		CONVERT(varchar(max), UKONTO_GILTIG_TOM, 126) AS ukonto_giltig_tom,
		CAST(UKONTO_ID AS VARCHAR(MAX)) AS ukonto_id,
		CAST(UKONTO_ID_TEXT AS VARCHAR(MAX)) AS ukonto_id_text,
		CAST(UKONTO_PASSIV AS VARCHAR(MAX)) AS ukonto_passiv,
		CAST(UKONTO_TEXT AS VARCHAR(MAX)) AS ukonto_text 
	FROM Utdata.udp_100.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    
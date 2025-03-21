
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FMK_GILTIG_FOM': 'varchar(max)', 'FMK_GILTIG_TOM': 'varchar(max)', 'FMK_ID': 'varchar(max)', 'FMK_ID_TEXT': 'varchar(max)', 'FMK_PASSIV': 'varchar(max)', 'FMK_TEXT': 'varchar(max)', 'FRA01_GILTIG_FOM': 'varchar(max)', 'FRA01_GILTIG_TOM': 'varchar(max)', 'FRA01_ID': 'varchar(max)', 'FRA01_ID_TEXT': 'varchar(max)', 'FRA01_PASSIV': 'varchar(max)', 'FRA01_TEXT': 'varchar(max)', 'FRA03_GILTIG_FOM': 'varchar(max)', 'FRA03_GILTIG_TOM': 'varchar(max)', 'FRA03_ID': 'varchar(max)', 'FRA03_ID_TEXT': 'varchar(max)', 'FRA03_PASSIV': 'varchar(max)', 'FRA03_TEXT': 'varchar(max)', 'FRA05_GILTIG_FOM': 'varchar(max)', 'FRA05_GILTIG_TOM': 'varchar(max)', 'FRA05_ID': 'varchar(max)', 'FRA05_ID_TEXT': 'varchar(max)', 'FRA05_PASSIV': 'varchar(max)', 'FRA05_TEXT': 'varchar(max)', 'KBR_GILTIG_FOM': 'varchar(max)', 'KBR_GILTIG_TOM': 'varchar(max)', 'KBR_ID': 'varchar(max)', 'KBR_ID_TEXT': 'varchar(max)', 'KBR_PASSIV': 'varchar(max)', 'KBR_TEXT': 'varchar(max)', 'KONCRR_GILTIG_FOM': 'varchar(max)', 'KONCRR_GILTIG_TOM': 'varchar(max)', 'KONCRR_ID': 'varchar(max)', 'KONCRR_ID_TEXT': 'varchar(max)', 'KONCRR_PASSIV': 'varchar(max)', 'KONCRR_TEXT': 'varchar(max)', 'KONDEL_GILTIG_FOM': 'varchar(max)', 'KONDEL_GILTIG_TOM': 'varchar(max)', 'KONDEL_ID': 'varchar(max)', 'KONDEL_ID_TEXT': 'varchar(max)', 'KONDEL_PASSIV': 'varchar(max)', 'KONDEL_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'KTO1_GILTIG_FOM': 'varchar(max)', 'KTO1_GILTIG_TOM': 'varchar(max)', 'KTO1_ID': 'varchar(max)', 'KTO1_ID_TEXT': 'varchar(max)', 'KTO1_PASSIV': 'varchar(max)', 'KTO1_TEXT': 'varchar(max)', 'KTO2_GILTIG_FOM': 'varchar(max)', 'KTO2_GILTIG_TOM': 'varchar(max)', 'KTO2_ID': 'varchar(max)', 'KTO2_ID_TEXT': 'varchar(max)', 'KTO2_PASSIV': 'varchar(max)', 'KTO2_TEXT': 'varchar(max)', 'KTYP_GILTIG_FOM': 'varchar(max)', 'KTYP_GILTIG_TOM': 'varchar(max)', 'KTYP_ID': 'varchar(max)', 'KTYP_ID_TEXT': 'varchar(max)', 'KTYP_PASSIV': 'varchar(max)', 'KTYP_TEXT': 'varchar(max)', 'MKTO_GILTIG_FOM': 'varchar(max)', 'MKTO_GILTIG_TOM': 'varchar(max)', 'MKTO_ID': 'varchar(max)', 'MKTO_ID_TEXT': 'varchar(max)', 'MKTO_PASSIV': 'varchar(max)', 'MKTO_TEXT': 'varchar(max)', 'MS_GILTIG_FOM': 'varchar(max)', 'MS_GILTIG_TOM': 'varchar(max)', 'MS_ID': 'varchar(max)', 'MS_ID_TEXT': 'varchar(max)', 'MS_PASSIV': 'varchar(max)', 'MS_TEXT': 'varchar(max)', 'SA_GILTIG_FOM': 'varchar(max)', 'SA_GILTIG_TOM': 'varchar(max)', 'SA_ID': 'varchar(max)', 'SA_ID_TEXT': 'varchar(max)', 'SA_PASSIV': 'varchar(max)', 'SA_TEXT': 'varchar(max)', 'SKRD_GILTIG_FOM': 'varchar(max)', 'SKRD_GILTIG_TOM': 'varchar(max)', 'SKRD_ID': 'varchar(max)', 'SKRD_ID_TEXT': 'varchar(max)', 'SKRD_PASSIV': 'varchar(max)', 'SKRD_TEXT': 'varchar(max)', 'SRU_GILTIG_FOM': 'varchar(max)', 'SRU_GILTIG_TOM': 'varchar(max)', 'SRU_ID': 'varchar(max)', 'SRU_ID_TEXT': 'varchar(max)', 'SRU_PASSIV': 'varchar(max)', 'SRU_TEXT': 'varchar(max)', 'ST_GILTIG_FOM': 'varchar(max)', 'ST_GILTIG_TOM': 'varchar(max)', 'ST_ID': 'varchar(max)', 'ST_ID_TEXT': 'varchar(max)', 'ST_PASSIV': 'varchar(max)', 'ST_TEXT': 'varchar(max)', 'SÖSRR_GILTIG_FOM': 'varchar(max)', 'SÖSRR_GILTIG_TOM': 'varchar(max)', 'SÖSRR_ID': 'varchar(max)', 'SÖSRR_ID_TEXT': 'varchar(max)', 'SÖSRR_PASSIV': 'varchar(max)', 'SÖSRR_TEXT': 'varchar(max)', 'UKTO_GILTIG_FOM': 'varchar(max)', 'UKTO_GILTIG_TOM': 'varchar(max)', 'UKTO_ID': 'varchar(max)', 'UKTO_ID_TEXT': 'varchar(max)', 'UKTO_PASSIV': 'varchar(max)', 'UKTO_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), FMK_GILTIG_FOM, 126) AS fmk_giltig_fom,
		CONVERT(varchar(max), FMK_GILTIG_TOM, 126) AS fmk_giltig_tom,
		CAST(FMK_ID AS VARCHAR(MAX)) AS fmk_id,
		CAST(FMK_ID_TEXT AS VARCHAR(MAX)) AS fmk_id_text,
		CAST(FMK_PASSIV AS VARCHAR(MAX)) AS fmk_passiv,
		CAST(FMK_TEXT AS VARCHAR(MAX)) AS fmk_text,
		CONVERT(varchar(max), FRA01_GILTIG_FOM, 126) AS fra01_giltig_fom,
		CONVERT(varchar(max), FRA01_GILTIG_TOM, 126) AS fra01_giltig_tom,
		CAST(FRA01_ID AS VARCHAR(MAX)) AS fra01_id,
		CAST(FRA01_ID_TEXT AS VARCHAR(MAX)) AS fra01_id_text,
		CAST(FRA01_PASSIV AS VARCHAR(MAX)) AS fra01_passiv,
		CAST(FRA01_TEXT AS VARCHAR(MAX)) AS fra01_text,
		CONVERT(varchar(max), FRA03_GILTIG_FOM, 126) AS fra03_giltig_fom,
		CONVERT(varchar(max), FRA03_GILTIG_TOM, 126) AS fra03_giltig_tom,
		CAST(FRA03_ID AS VARCHAR(MAX)) AS fra03_id,
		CAST(FRA03_ID_TEXT AS VARCHAR(MAX)) AS fra03_id_text,
		CAST(FRA03_PASSIV AS VARCHAR(MAX)) AS fra03_passiv,
		CAST(FRA03_TEXT AS VARCHAR(MAX)) AS fra03_text,
		CONVERT(varchar(max), FRA05_GILTIG_FOM, 126) AS fra05_giltig_fom,
		CONVERT(varchar(max), FRA05_GILTIG_TOM, 126) AS fra05_giltig_tom,
		CAST(FRA05_ID AS VARCHAR(MAX)) AS fra05_id,
		CAST(FRA05_ID_TEXT AS VARCHAR(MAX)) AS fra05_id_text,
		CAST(FRA05_PASSIV AS VARCHAR(MAX)) AS fra05_passiv,
		CAST(FRA05_TEXT AS VARCHAR(MAX)) AS fra05_text,
		CONVERT(varchar(max), KBR_GILTIG_FOM, 126) AS kbr_giltig_fom,
		CONVERT(varchar(max), KBR_GILTIG_TOM, 126) AS kbr_giltig_tom,
		CAST(KBR_ID AS VARCHAR(MAX)) AS kbr_id,
		CAST(KBR_ID_TEXT AS VARCHAR(MAX)) AS kbr_id_text,
		CAST(KBR_PASSIV AS VARCHAR(MAX)) AS kbr_passiv,
		CAST(KBR_TEXT AS VARCHAR(MAX)) AS kbr_text,
		CONVERT(varchar(max), KONCRR_GILTIG_FOM, 126) AS koncrr_giltig_fom,
		CONVERT(varchar(max), KONCRR_GILTIG_TOM, 126) AS koncrr_giltig_tom,
		CAST(KONCRR_ID AS VARCHAR(MAX)) AS koncrr_id,
		CAST(KONCRR_ID_TEXT AS VARCHAR(MAX)) AS koncrr_id_text,
		CAST(KONCRR_PASSIV AS VARCHAR(MAX)) AS koncrr_passiv,
		CAST(KONCRR_TEXT AS VARCHAR(MAX)) AS koncrr_text,
		CONVERT(varchar(max), KONDEL_GILTIG_FOM, 126) AS kondel_giltig_fom,
		CONVERT(varchar(max), KONDEL_GILTIG_TOM, 126) AS kondel_giltig_tom,
		CAST(KONDEL_ID AS VARCHAR(MAX)) AS kondel_id,
		CAST(KONDEL_ID_TEXT AS VARCHAR(MAX)) AS kondel_id_text,
		CAST(KONDEL_PASSIV AS VARCHAR(MAX)) AS kondel_passiv,
		CAST(KONDEL_TEXT AS VARCHAR(MAX)) AS kondel_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), KTO1_GILTIG_FOM, 126) AS kto1_giltig_fom,
		CONVERT(varchar(max), KTO1_GILTIG_TOM, 126) AS kto1_giltig_tom,
		CAST(KTO1_ID AS VARCHAR(MAX)) AS kto1_id,
		CAST(KTO1_ID_TEXT AS VARCHAR(MAX)) AS kto1_id_text,
		CAST(KTO1_PASSIV AS VARCHAR(MAX)) AS kto1_passiv,
		CAST(KTO1_TEXT AS VARCHAR(MAX)) AS kto1_text,
		CONVERT(varchar(max), KTO2_GILTIG_FOM, 126) AS kto2_giltig_fom,
		CONVERT(varchar(max), KTO2_GILTIG_TOM, 126) AS kto2_giltig_tom,
		CAST(KTO2_ID AS VARCHAR(MAX)) AS kto2_id,
		CAST(KTO2_ID_TEXT AS VARCHAR(MAX)) AS kto2_id_text,
		CAST(KTO2_PASSIV AS VARCHAR(MAX)) AS kto2_passiv,
		CAST(KTO2_TEXT AS VARCHAR(MAX)) AS kto2_text,
		CONVERT(varchar(max), KTYP_GILTIG_FOM, 126) AS ktyp_giltig_fom,
		CONVERT(varchar(max), KTYP_GILTIG_TOM, 126) AS ktyp_giltig_tom,
		CAST(KTYP_ID AS VARCHAR(MAX)) AS ktyp_id,
		CAST(KTYP_ID_TEXT AS VARCHAR(MAX)) AS ktyp_id_text,
		CAST(KTYP_PASSIV AS VARCHAR(MAX)) AS ktyp_passiv,
		CAST(KTYP_TEXT AS VARCHAR(MAX)) AS ktyp_text,
		CONVERT(varchar(max), MKTO_GILTIG_FOM, 126) AS mkto_giltig_fom,
		CONVERT(varchar(max), MKTO_GILTIG_TOM, 126) AS mkto_giltig_tom,
		CAST(MKTO_ID AS VARCHAR(MAX)) AS mkto_id,
		CAST(MKTO_ID_TEXT AS VARCHAR(MAX)) AS mkto_id_text,
		CAST(MKTO_PASSIV AS VARCHAR(MAX)) AS mkto_passiv,
		CAST(MKTO_TEXT AS VARCHAR(MAX)) AS mkto_text,
		CONVERT(varchar(max), MS_GILTIG_FOM, 126) AS ms_giltig_fom,
		CONVERT(varchar(max), MS_GILTIG_TOM, 126) AS ms_giltig_tom,
		CAST(MS_ID AS VARCHAR(MAX)) AS ms_id,
		CAST(MS_ID_TEXT AS VARCHAR(MAX)) AS ms_id_text,
		CAST(MS_PASSIV AS VARCHAR(MAX)) AS ms_passiv,
		CAST(MS_TEXT AS VARCHAR(MAX)) AS ms_text,
		CONVERT(varchar(max), SA_GILTIG_FOM, 126) AS sa_giltig_fom,
		CONVERT(varchar(max), SA_GILTIG_TOM, 126) AS sa_giltig_tom,
		CAST(SA_ID AS VARCHAR(MAX)) AS sa_id,
		CAST(SA_ID_TEXT AS VARCHAR(MAX)) AS sa_id_text,
		CAST(SA_PASSIV AS VARCHAR(MAX)) AS sa_passiv,
		CAST(SA_TEXT AS VARCHAR(MAX)) AS sa_text,
		CONVERT(varchar(max), SKRD_GILTIG_FOM, 126) AS skrd_giltig_fom,
		CONVERT(varchar(max), SKRD_GILTIG_TOM, 126) AS skrd_giltig_tom,
		CAST(SKRD_ID AS VARCHAR(MAX)) AS skrd_id,
		CAST(SKRD_ID_TEXT AS VARCHAR(MAX)) AS skrd_id_text,
		CAST(SKRD_PASSIV AS VARCHAR(MAX)) AS skrd_passiv,
		CAST(SKRD_TEXT AS VARCHAR(MAX)) AS skrd_text,
		CONVERT(varchar(max), SRU_GILTIG_FOM, 126) AS sru_giltig_fom,
		CONVERT(varchar(max), SRU_GILTIG_TOM, 126) AS sru_giltig_tom,
		CAST(SRU_ID AS VARCHAR(MAX)) AS sru_id,
		CAST(SRU_ID_TEXT AS VARCHAR(MAX)) AS sru_id_text,
		CAST(SRU_PASSIV AS VARCHAR(MAX)) AS sru_passiv,
		CAST(SRU_TEXT AS VARCHAR(MAX)) AS sru_text,
		CONVERT(varchar(max), ST_GILTIG_FOM, 126) AS st_giltig_fom,
		CONVERT(varchar(max), ST_GILTIG_TOM, 126) AS st_giltig_tom,
		CAST(ST_ID AS VARCHAR(MAX)) AS st_id,
		CAST(ST_ID_TEXT AS VARCHAR(MAX)) AS st_id_text,
		CAST(ST_PASSIV AS VARCHAR(MAX)) AS st_passiv,
		CAST(ST_TEXT AS VARCHAR(MAX)) AS st_text,
		CONVERT(varchar(max), SÖSRR_GILTIG_FOM, 126) AS sösrr_giltig_fom,
		CONVERT(varchar(max), SÖSRR_GILTIG_TOM, 126) AS sösrr_giltig_tom,
		CAST(SÖSRR_ID AS VARCHAR(MAX)) AS sösrr_id,
		CAST(SÖSRR_ID_TEXT AS VARCHAR(MAX)) AS sösrr_id_text,
		CAST(SÖSRR_PASSIV AS VARCHAR(MAX)) AS sösrr_passiv,
		CAST(SÖSRR_TEXT AS VARCHAR(MAX)) AS sösrr_text,
		CONVERT(varchar(max), UKTO_GILTIG_FOM, 126) AS ukto_giltig_fom,
		CONVERT(varchar(max), UKTO_GILTIG_TOM, 126) AS ukto_giltig_tom,
		CAST(UKTO_ID AS VARCHAR(MAX)) AS ukto_id,
		CAST(UKTO_ID_TEXT AS VARCHAR(MAX)) AS ukto_id_text,
		CAST(UKTO_PASSIV AS VARCHAR(MAX)) AS ukto_passiv,
		CAST(UKTO_TEXT AS VARCHAR(MAX)) AS ukto_text 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    
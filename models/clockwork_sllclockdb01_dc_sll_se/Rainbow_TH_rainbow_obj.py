
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date',
 '_metadata_modified_utc': 'datetime2',
 '_source_catalog': 'varchar(max)',
 'actpas': 'varchar(max)',
 'altcod': 'varchar(max)',
 'altr01': 'varchar(max)',
 'altr02': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'extcod': 'varchar(max)',
 'gencom': 'varchar(max)',
 'hidsrc': 'varchar(max)',
 'infmdl': 'varchar(max)',
 'isocod': 'varchar(max)',
 'namdes': 'varchar(max)',
 'objcod': 'varchar(max)',
 'objn01': 'varchar(max)',
 'objn02': 'varchar(max)',
 'objp01': 'varchar(max)',
 'objt01': 'varchar(max)',
 'objt02': 'varchar(max)',
 'objt04': 'varchar(max)',
 'objtyp': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'text01': 'varchar(max)',
 'txtdsc': 'varchar(max)',
 'valfrm': 'varchar(max)',
 'valunt': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=1,
        time_column="_data_modified_utc"
    ),
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_Rainbow_TH_rainbow_obj_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.Rainbow_TH_rainbow_obj (_data_modified_utc)"]
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
 		CAST(
        CAST(
            COALESCE(
                CASE
                    WHEN credat > chgdat or chgdat IS NULL then credat
                    WHEN chgdat > credat or credat is NULL then chgdat
                    ELSE credat
                END,
                chgdat,
                credat
            ) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC'
        AS datetime2
    ) AS DATE ) as data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'Rainbow_TH' as _source_catalog,
		CAST(actpas AS VARCHAR(MAX)) AS actpas,
		CAST(altcod AS VARCHAR(MAX)) AS altcod,
		CAST(altr01 AS VARCHAR(MAX)) AS altr01,
		CAST(altr02 AS VARCHAR(MAX)) AS altr02,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(extcod AS VARCHAR(MAX)) AS extcod,
		CAST(gencom AS VARCHAR(MAX)) AS gencom,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(infmdl AS VARCHAR(MAX)) AS infmdl,
		CAST(isocod AS VARCHAR(MAX)) AS isocod,
		CAST(namdes AS VARCHAR(MAX)) AS namdes,
		CAST(objcod AS VARCHAR(MAX)) AS objcod,
		CAST(objn01 AS VARCHAR(MAX)) AS objn01,
		CAST(objn02 AS VARCHAR(MAX)) AS objn02,
		CAST(objp01 AS VARCHAR(MAX)) AS objp01,
		CAST(objt01 AS VARCHAR(MAX)) AS objt01,
		CAST(objt02 AS VARCHAR(MAX)) AS objt02,
		CAST(objt04 AS VARCHAR(MAX)) AS objt04,
		CAST(objtyp AS VARCHAR(MAX)) AS objtyp,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(text01 AS VARCHAR(MAX)) AS text01,
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc,
		CONVERT(varchar(max), valfrm, 126) AS valfrm,
		CONVERT(varchar(max), valunt, 126) AS valunt 
	FROM Rainbow_TH.rainbow.obj
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    
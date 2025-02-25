
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'actpas': 'varchar(max)',
 'altcod': 'varchar(max)',
 'altr01': 'varchar(max)',
 'altr02': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'data_modified': 'date',
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
 'source_catalog': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'text01': 'varchar(max)',
 'txtdsc': 'varchar(max)',
 'valfrm': 'varchar(max)',
 'valunt': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="data_modified"
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
    query = """
	SELECT 
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
		CONVERT(varchar(max), valunt, 126) AS valunt,
		CAST(
                COALESCE(
                    CASE
                        WHEN credat > chgdat OR chgdat IS NULL THEN credat
                        WHEN chgdat > credat OR credat IS NULL THEN chgdat
                        ELSE credat
                    END,
                    chgdat,
                    credat
                ) AS DATE
            ) AS data_modified,
		'Rainbow_SLSO' as source_catalog 
	FROM Rainbow_SLSO.rainbow.obj
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        
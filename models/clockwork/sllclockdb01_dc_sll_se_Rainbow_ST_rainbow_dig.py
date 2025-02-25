
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'data_modified': 'date',
 'digcod': 'varchar(max)',
 'dignam': 'varchar(max)',
 'migcod': 'varchar(max)',
 'sigcod': 'varchar(max)',
 'source_catalog': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'txtdsc': 'varchar(max)'},
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
 		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(digcod AS VARCHAR(MAX)) AS digcod,
		CAST(dignam AS VARCHAR(MAX)) AS dignam,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(sigcod AS VARCHAR(MAX)) AS sigcod,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc,
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
		'Rainbow_ST' as source_catalog 
	FROM Rainbow_ST.rainbow.dig
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        
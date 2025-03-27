
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'digcod': 'varchar(max)', 'dignam': 'varchar(max)', 'migcod': 'varchar(max)', 'sigcod': 'varchar(max)', 'srtnam': 'varchar(max)', 'srtnum': 'varchar(max)', 'txtdsc': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=5000,
        time_column="_data_modified_utc"
    ),
    start=start,
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_slso_rainbow_dig_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_slso_rainbow_dig (_data_modified_utc)"]
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
    ) AS DATE ) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'Rainbow_SLSO' as _source_catalog,
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
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc 
	FROM Rainbow_SLSO.rainbow.dig
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    
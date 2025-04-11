
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'defacc': 'varchar(max)', 'defcod': 'varchar(max)', 'defdim': 'varchar(max)', 'deftyp': 'varchar(max)', 'dficod': 'varchar(max)', 'dfiseq': 'varchar(max)', 'entdat': 'varchar(max)', 'entfrm': 'varchar(max)', 'entpgm': 'varchar(max)', 'entusr': 'varchar(max)', 'seqnum': 'varchar(max)', 'taxtyp': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'defacc', 'defdim', 'deftyp', 'dficod', 'dfiseq', 'seqnum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_slso_rainbow_dfitrn_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_slso_rainbow_dfitrn (_data_modified_utc)"]
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
		CAST(defacc AS VARCHAR(MAX)) AS defacc,
		CAST(defcod AS VARCHAR(MAX)) AS defcod,
		CAST(defdim AS VARCHAR(MAX)) AS defdim,
		CAST(deftyp AS VARCHAR(MAX)) AS deftyp,
		CAST(dficod AS VARCHAR(MAX)) AS dficod,
		CAST(dfiseq AS VARCHAR(MAX)) AS dfiseq,
		CONVERT(varchar(max), entdat, 126) AS entdat,
		CAST(entfrm AS VARCHAR(MAX)) AS entfrm,
		CAST(entpgm AS VARCHAR(MAX)) AS entpgm,
		CAST(entusr AS VARCHAR(MAX)) AS entusr,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(taxtyp AS VARCHAR(MAX)) AS taxtyp 
	FROM Rainbow_SLSO.rainbow.dfitrn
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Provmaterial som kan kopplas till en beställning (Virologlabb)""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'IsLocalizationRequired': 'varchar(max)', 'IsOrderable': 'varchar(max)', 'IsPriorityOrderShown': 'varchar(max)', 'Specimen': 'varchar(max)', 'SpecimenCode': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'SpecimenID': "{'title_ui': None, 'description': 'Provmaterialets id'}", 'Specimen': "{'title_ui': None, 'description': 'Provmaterial'}", 'SpecimenCode': "{'title_ui': None, 'description': 'Unikt ID för provmaterialet'}", 'IsLocalizationRequired': "{'title_ui': None, 'description': 'Lokalisation ska visas och måste fyllas i'}", 'IsPriorityOrderShown': "{'title_ui': None, 'description': 'Prioordning kan fyllas i'}", 'IsOrderable': "{'title_ui': None, 'description': 'Beställningsbar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
 		CAST(CAST(TimestampRead AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(IsLocalizationRequired AS VARCHAR(MAX)) AS IsLocalizationRequired,
		CAST(IsOrderable AS VARCHAR(MAX)) AS IsOrderable,
		CAST(IsPriorityOrderShown AS VARCHAR(MAX)) AS IsPriorityOrderShown,
		CAST(Specimen AS VARCHAR(MAX)) AS Specimen,
		CAST(SpecimenCode AS VARCHAR(MAX)) AS SpecimenCode,
		CAST(SpecimenID AS VARCHAR(MAX)) AS SpecimenID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_VirSpecimens) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    
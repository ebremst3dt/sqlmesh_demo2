
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Resultat från analyser per provtagningstilfälle.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AnalysisRow': 'varchar(max)', 'DocumentID': 'varchar(max)', 'GroupRow': 'varchar(max)', 'IsDeviating': 'varchar(max)', 'OccasionRow': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'GroupRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OccasionRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'AnalysisRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Value': "{'title_ui': None, 'description': 'Värde på resultatet av labanalysen. Ibland en sträng, ibland ett tal.'}", 'IsDeviating': "{'title_ui': '*', 'description': 'Om värdet är utanför referensintervall. True och NULL visas som avvikande (*) i TakeCare.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AnalysisRow AS VARCHAR(MAX)) AS AnalysisRow,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(GroupRow AS VARCHAR(MAX)) AS GroupRow,
		CAST(IsDeviating AS VARCHAR(MAX)) AS IsDeviating,
		CAST(OccasionRow AS VARCHAR(MAX)) AS OccasionRow,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Value AS VARCHAR(MAX)) AS Value,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPharmacologyReplies_AnalysesOccasions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="En tidstyp anger vilken typ av aktivitet som en tid är reserverad för och kan vara t.ex "Rond" eller "Mottagning". Tidstyper definieras per vårdenhet. Vissa tidstyper existerar bara i TakeCares gränssnitt.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CareUnitID': 'varchar(max)', 'ColourCodeBlue': 'varchar(max)', 'ColourCodeGreen': 'varchar(max)', 'ColourCodeRed': 'varchar(max)', 'HasSelfCheckIn': 'varchar(max)', 'IsBookable': 'varchar(max)', 'IsHiddenForCounter': 'varchar(max)', 'IsHiddenForPatient': 'varchar(max)', 'IsWebBookable': 'varchar(max)', 'IsWebCancellable': 'varchar(max)', 'IsWebRebookable': 'varchar(max)', 'Name': 'varchar(max)', 'Purpose': 'varchar(max)', 'TimeTypeID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'WebName': 'varchar(max)'},
    column_descriptions={'TimeTypeID': "{'title_ui': None, 'description': None}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'Name': "{'title_ui': 'Namn i TakeCare', 'description': None}", 'IsBookable': '{\'title_ui\': \'Bokningsbar TakeCare\', \'description\': \'Vissa tider är inte bokningsbara t.ex "Rond", för då vill man inte ha andra saker inbokade\'}', 'IsWebBookable': "{'title_ui': 'Bokningsbar webb', 'description': 'Om tidstypen är internetbokningsbar'}", 'IsHiddenForPatient': "{'title_ui': 'Dölj bokning för patient', 'description': 'Om tidstypen inte skall visas för patienten'}", 'IsHiddenForCounter': "{'title_ui': 'Dölj i kassa', 'description': 'Om tidstypen inte skall visas i kassan'}", 'HasSelfCheckIn': "{'title_ui': 'Självincheckningsbar', 'description': 'Om tidstypen tillåter patienten att självinchecka'}", 'WebName': "{'title_ui': 'Namn för webbtidbok', 'description': 'Namn som visas för webbtjänster istället för Name. Om WebName är tomt används värdet i Name.'}", 'IsWebCancellable': "{'title_ui': 'Kan avbokas från webben', 'description': 'Om tidstypen är avbokningsbar från webben'}", 'IsWebRebookable': "{'title_ui': 'Kan ombokas från webben', 'description': 'Om tidstypen är ombokningsbar från webben'}", 'Purpose': "{'title_ui': 'Informationstext för webb', 'description': 'Texten i det här fältet visas för patienten via externa tjänster och motsvarar en kallelsetext'}", 'ColourCodeRed': "{'title_ui': 'Färg', 'description': 'Färg (röd) i RGB'}", 'ColourCodeGreen': "{'title_ui': 'Färg', 'description': 'Färg (grön) i RGB'}", 'ColourCodeBlue': "{'title_ui': 'Färg', 'description': 'Färg (blå) i RGB'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CAST(ColourCodeBlue AS VARCHAR(MAX)) AS ColourCodeBlue,
		CAST(ColourCodeGreen AS VARCHAR(MAX)) AS ColourCodeGreen,
		CAST(ColourCodeRed AS VARCHAR(MAX)) AS ColourCodeRed,
		CAST(HasSelfCheckIn AS VARCHAR(MAX)) AS HasSelfCheckIn,
		CAST(IsBookable AS VARCHAR(MAX)) AS IsBookable,
		CAST(IsHiddenForCounter AS VARCHAR(MAX)) AS IsHiddenForCounter,
		CAST(IsHiddenForPatient AS VARCHAR(MAX)) AS IsHiddenForPatient,
		CAST(IsWebBookable AS VARCHAR(MAX)) AS IsWebBookable,
		CAST(IsWebCancellable AS VARCHAR(MAX)) AS IsWebCancellable,
		CAST(IsWebRebookable AS VARCHAR(MAX)) AS IsWebRebookable,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(Purpose AS VARCHAR(MAX)) AS Purpose,
		CAST(TimeTypeID AS VARCHAR(MAX)) AS TimeTypeID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(WebName AS VARCHAR(MAX)) AS WebName 
	FROM Intelligence.viewreader.vCodes_ResourceTimeTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    

import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Termer i termkatalogen. Termer kan höra till olika kategorier, vilket avgör var i TakeCare de används. De kan också ha olika datatyper, vilket avgör hur de matas in. Termer ska aldrig byta datatyp, villkor etc, under sin livslängd.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'ConditionID': 'varchar(max)', 'DataTypeID': 'varchar(max)', 'Description': 'varchar(max)', 'Formatting': 'varchar(max)', 'Formula': 'varchar(max)', 'IsAdministrative': 'varchar(max)', 'IsClassifiableMainSecDiagnosis': 'varchar(max)', 'IsConcept': 'varchar(max)', 'IsFormTerm': 'varchar(max)', 'IsKeyword': 'varchar(max)', 'IsLabAnalysis': 'varchar(max)', 'IsMeasurement': 'varchar(max)', 'IsShownInMeasurementAndLab': 'varchar(max)', 'IsTask': 'varchar(max)', 'IsValueTerm': 'varchar(max)', 'Name': 'varchar(max)', 'Note': 'varchar(max)', 'Operand1': 'varchar(max)', 'Operand2': 'varchar(max)', 'PlausibilityLimitMax': 'varchar(max)', 'PlausibilityLimitMin': 'varchar(max)', 'ReferenceLiterature': 'varchar(max)', 'StartDate': 'varchar(max)', 'TermCode': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'TermID': "{'title_ui': 'Term id', 'description': 'Termens id'}", 'Name': "{'title_ui': 'Termnamn', 'description': 'Själva termen'}", 'Description': "{'title_ui': 'Definition', 'description': None}", 'Comment': "{'title_ui': 'Användningsområde/Kommentar', 'description': None}", 'ReferenceLiterature': "{'title_ui': 'Källa', 'description': None}", 'Note': "{'title_ui': 'Anteckningar för termansvarig', 'description': 'Anteckningar som bara visas i termkatalogens administrationsgränssnitt.'}", 'Unit': "{'title_ui': 'Enhet', 'description': 'Enhet för värde'}", 'DataTypeID': "{'title_ui': 'Datatyp', 'description': 'Termens datatyp'}", 'IsKeyword': "{'title_ui': 'Sökord', 'description': 'Om termen ska kunna användas i journaltext'}", 'IsValueTerm': "{'title_ui': 'Värdeterm', 'description': 'Om termen ska kunna kopplas till andra termer som ett valbart värde'}", 'IsLabAnalysis': "{'title_ui': 'Labanalys', 'description': 'Om termen är en labbanalys. Används inte längre.'}", 'IsConcept': "{'title_ui': 'Begrepp', 'description': 'Om termen ska kunna kopplas till andra termer som ett begrepp'}", 'IsAdministrative': "{'title_ui': 'Adm. term', 'description': 'Om termen ska användas internt i systemet'}", 'IsTask': "{'title_ui': 'Aktivitet', 'description': 'Om termen ska kunna användas som en aktivitet'}", 'IsFormTerm': "{'title_ui': 'Formulärterm', 'description': 'Om termen ska kunna användas i formulär eller blanketter'}", 'IsMeasurement': "{'title_ui': 'Mätvärde', 'description': 'Om termen ska kunna användas i mätvärdesmodulen'}", 'StartDate': "{'title_ui': 'Giltighetstid fr.o.m.', 'description': 'Datum då termen börjar gälla'}", 'ValidThroughDate': "{'title_ui': 'Giltighetstid t.o.m.', 'description': 'Datum då termen slutar gälla'}", 'PlausibilityLimitMin': "{'title_ui': 'Rimlighetsgräns min', 'description': 'Gräns för inmatade numeriska värden (ger varning till användaren om detta värde underskrids)'}", 'PlausibilityLimitMax': "{'title_ui': 'Rimlighetsgräns max', 'description': 'Gräns för inmatade numeriska värden (ger varning till användaren om detta värde överskrids)'}", 'ConditionID': "{'title_ui': 'Villkor', 'description': 'Ett villkor som begränsar möjliga numeriska värden vid inmatning. Värden som inte uppfyller villkoret går inte att spara.'}", 'Operand1': "{'title_ui': None, 'description': 'Operand 1 för villkoret'}", 'Operand2': "{'title_ui': None, 'description': 'Operand 2 för villkoret'}", 'Formula': "{'title_ui': 'Funktion', 'description': 'Formel för genererat värde, enligt ett internt format'}", 'Formatting': "{'title_ui': 'Formatering', 'description': 'Formatering för genererat värde, enligt ett internt format'}", 'IsClassifiableMainSecDiagnosis': "{'title_ui': None, 'description': 'Termen kan klassificeras i Huvud och Bi-diagnos'}", 'TermCode': "{'title_ui': 'Termkod', 'description': 'Termens kod som sätts på systemnivå t.ex. från externt register'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(ConditionID AS VARCHAR(MAX)) AS ConditionID,
		CAST(DataTypeID AS VARCHAR(MAX)) AS DataTypeID,
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(Formatting AS VARCHAR(MAX)) AS Formatting,
		CAST(Formula AS VARCHAR(MAX)) AS Formula,
		CAST(IsAdministrative AS VARCHAR(MAX)) AS IsAdministrative,
		CAST(IsClassifiableMainSecDiagnosis AS VARCHAR(MAX)) AS IsClassifiableMainSecDiagnosis,
		CAST(IsConcept AS VARCHAR(MAX)) AS IsConcept,
		CAST(IsFormTerm AS VARCHAR(MAX)) AS IsFormTerm,
		CAST(IsKeyword AS VARCHAR(MAX)) AS IsKeyword,
		CAST(IsLabAnalysis AS VARCHAR(MAX)) AS IsLabAnalysis,
		CAST(IsMeasurement AS VARCHAR(MAX)) AS IsMeasurement,
		CAST(IsShownInMeasurementAndLab AS VARCHAR(MAX)) AS IsShownInMeasurementAndLab,
		CAST(IsTask AS VARCHAR(MAX)) AS IsTask,
		CAST(IsValueTerm AS VARCHAR(MAX)) AS IsValueTerm,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(Note AS VARCHAR(MAX)) AS Note,
		CAST(Operand1 AS VARCHAR(MAX)) AS Operand1,
		CAST(Operand2 AS VARCHAR(MAX)) AS Operand2,
		CAST(PlausibilityLimitMax AS VARCHAR(MAX)) AS PlausibilityLimitMax,
		CAST(PlausibilityLimitMin AS VARCHAR(MAX)) AS PlausibilityLimitMin,
		CAST(ReferenceLiterature AS VARCHAR(MAX)) AS ReferenceLiterature,
		CONVERT(varchar(max), StartDate, 126) AS StartDate,
		CAST(TermCode AS VARCHAR(MAX)) AS TermCode,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Unit AS VARCHAR(MAX)) AS Unit,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_Terms) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    
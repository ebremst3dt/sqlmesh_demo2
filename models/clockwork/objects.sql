
MODEL (
    kind VIEW
);

SELECT
    actpas AS 'active_slash_passive_status_code',
	altcod AS 'alternative_code_for_object_code',
	altr01 AS 'general_alternative_1dot_usage_depening_on_object_type',
	altr02 AS 'general_alternative_2dot_usage_depening_on_object_type',
	chgdat AS 'information_last_changed_date',
	chgusr AS 'information_last_changed_by_user',
	compny AS 'company_identification_code',
	credat AS 'information_created_date',
	creusr AS 'information_created_by_user',
	extcod AS 'object_code_in_external_system',
	gencom AS 'general_comments',
	hidsrc AS 'hide_object_from_search',
	infmdl AS 'information_model_identification_code',
	isocod AS 'iso_code_for_object',
	namdes AS 'object_name__slash__description',
	objcod AS 'object_identification_code',
	objn01 AS 'general_numeric_value_1dot_usage_depening_on_object_type',
	objn02 AS 'general_numeric_value_2dot_usage_depening_on_object_type',
	objp01 AS 'general_percentage_value_1dot_usage_depening_on_object_type',
	objt01 AS 'general_text_value_1dot_usage_depening_on_object_type',
	objt02 AS 'general_text_value_2dot_usage_depening_on_object_type',
	objt04 AS 'general_text_value_4dot_usage_depening_on_object_type',
	objtyp AS 'object_type',
	srtnam AS 'short_name',
	srtnum AS 'short_number',
	text01 AS 'general_text__slash__comment_1dot_usage_depening_on_object_type',
	txtdsc AS 'additional_text__slash__description',
	valfrm AS 'valid_from_date',
	valunt AS 'valid_until_date',
	data_modified,
	source_catalog
FROM clockwork.rainbow_obj
    
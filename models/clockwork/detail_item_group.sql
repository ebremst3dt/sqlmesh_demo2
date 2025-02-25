
MODEL (
    kind VIEW
);

SELECT
    chgdat AS 'information_last_changed_date',
	chgusr AS 'information_last_changed_by_user',
	compny AS 'company_identification_code',
	credat AS 'information_created_date',
	creusr AS 'information_created_by_user',
	digcod AS 'detail_item_group_identification_code',
	dignam AS 'name__slash__description',
	migcod AS 'main_item_group_identification_code',
	sigcod AS 'sub_item_group_identification_code',
	srtnam AS 'short_name',
	srtnum AS 'short_number',
	txtdsc AS 'additional_text__slash__description',
	data_modified,
	source_catalog
FROM clockwork.rainbow_dig
    
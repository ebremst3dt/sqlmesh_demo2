
MODEL (
    kind VIEW
);

SELECT
    chgdat AS 'information_last_changed_date',
	chgusr AS 'information_last_changed_by_user',
	compny AS 'company_identification_code',
	credat AS 'information_created_date',
	creusr AS 'information_created_by_user',
	digcod AS 'detailed_item_group_identification_code',
	gencom AS 'general_comments',
	hidsrc AS 'hide_item_classification_code_from_search',
	icscat AS 'item_classification_catalogue_identification_code',
	icscod AS 'classification_code',
	icsmap AS 'mapped__and__lined_up_classification',
	icsnam AS 'classification_name_slash_description',
	icsref AS 'item_classification_reference_code_in_catalogue',
	ikscat AS 'item_categorization_catalogue_identification_code',
	iksref AS 'lined_up_categorization_reference',
	lvlcod AS 'classification_level',
	migcod AS 'main_item_group_identification_code',
	ofmcod AS 'order_form_identification_code',
	parseq AS 'parent_sequence_number',
	prbuac AS 'purchase_budget_account_identification_code',
	seqnum AS 'sequence_number',
	sigcod AS 'sub_item_group_identification_code',
	txtdsc AS 'additional_text__slash__description',
	data_modified,
	source_catalog
FROM clockwork.rainbow_icscat

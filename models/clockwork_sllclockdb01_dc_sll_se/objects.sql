MODEL (
  column_descriptions (
    active_slash_passive_status_code = 'Active/Passive Status Code',
    alternative_code_for_object_code = 'Alternative Code for Object Code',
    general_alternative_1dot_usage_depening_on_object_type = 'General Alternative 1. Usage depening on Object Type',
    general_alternative_2dot_usage_depening_on_object_type = 'General Alternative 2. Usage depening on Object Type',
    information_last_changed_date = 'Information Last Changed Date',
    information_last_changed_by_user = 'Information Last Changed by User',
    company_identification_code = 'Company identification code',
    information_created_date = 'Information Created Date',
    information_created_by_user = 'Information Created by User',
    object_code_in_external_system = 'Object Code in External System',
    general_comments = 'General Comments',
    hide_object_from_search = 'Hide object from Search',
    information_model_identification_code = 'Information Model Identification Code',
    iso_code_for_object = 'ISO Code for Object',
    object_name__slash__description = 'Object Name / Description',
    object_identification_code = 'Object Identification Code',
    general_numeric_value_1dot_usage_depening_on_object_type = 'General Numeric Value 1. Usage depening on Object Type',
    general_numeric_value_2dot_usage_depening_on_object_type = 'General Numeric Value 2. Usage depening on Object Type',
    general_percentage_value_1dot_usage_depening_on_object_type = 'General Percentage Value 1. Usage depening on Object Type',
    general_text_value_1dot_usage_depening_on_object_type = 'General Text Value 1. Usage depening on Object Type',
    general_text_value_2dot_usage_depening_on_object_type = 'General Text Value 2. Usage depening on Object Type',
    general_text_value_4dot_usage_depening_on_object_type = 'General Text Value 4. Usage depening on Object Type',
    object_type = 'Object Type',
    short_name = 'Short Name',
    short_number = 'Short Number',
    general_text__slash__comment_1dot_usage_depening_on_object_type = 'General Text / Comment 1. Usage depening on Object Type',
    additional_text__slash__description = 'Additional Text / Description',
    valid_from_date = 'Valid from Date',
    valid_until_date = 'Valid until Date'
  )
);

SELECT
  actpas AS Active_slash_Passive_Status_Code,
  txtdsc AS Additional_Text__slash__Description,
  altcod AS Alternative_Code_for_Object_Code,
  compny AS Company_identification_code,
  altr01 AS General_Alternative_1dot_Usage_depening_on_Object_Type,
  altr02 AS General_Alternative_2dot_Usage_depening_on_Object_Type,
  gencom AS General_Comments,
  objn01 AS General_Numeric_Value_1dot_Usage_depening_on_Object_Type,
  objn02 AS General_Numeric_Value_2dot_Usage_depening_on_Object_Type,
  objp01 AS General_Percentage_Value_1dot_Usage_depening_on_Object_Type,
  objt01 AS General_Text_Value_1dot_Usage_depening_on_Object_Type,
  objt02 AS General_Text_Value_2dot_Usage_depening_on_Object_Type,
  objt04 AS General_Text_Value_4dot_Usage_depening_on_Object_Type,
  text01 AS General_Text__slash__Comment_1dot_Usage_depening_on_Object_Type,
  hidsrc AS Hide_object_from_Search,
  isocod AS ISO_Code_for_Object,
  credat AS Information_Created_Date,
  creusr AS Information_Created_by_User,
  chgdat AS Information_Last_Changed_Date,
  chgusr AS Information_Last_Changed_by_User,
  infmdl AS Information_Model_Identification_Code,
  extcod AS Object_Code_in_External_System,
  objcod AS Object_Identification_Code,
  namdes AS Object_Name__slash__Description,
  objtyp AS Object_Type,
  srtnam AS Short_Name,
  srtnum AS Short_Number,
  valfrm AS Valid_from_Date,
  valunt AS Valid_until_Date
FROM clockwork_sllclockdb01_dc_sll_se.rainbow_obj
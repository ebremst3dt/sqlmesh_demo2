MODEL (
  column_descriptions (
    _data_modified_utc = 'When data was modified at source.',
    _metadata_modified_utc = 'When data was inserted here.',
    _source_catalog = 'Which catalog the source came from.',
    additional_text__slash__description = 'Additional Text / Description',
    company_identification_code = 'Company Identification Code',
    detail_item_group_identification_code = 'Detail Item Group Identification Code',
    information_created_date = 'Information Created Date',
    information_created_by_user = 'Information Created by User',
    information_last_changed_date = 'Information Last Changed Date',
    information_last_changed_by_user = 'Information Last Changed by User',
    main_item_group_identification_code = 'Main Item Group Identification Code',
    name__slash__description = 'Name / Description',
    short_name = 'Short Name',
    short_number = 'Short Number',
    sub_item_group_identification_code = 'Sub Item Group Identification Code'
  )
);

SELECT
  _data_modified_utc AS _data_modified_utc,
  _metadata_modified_utc AS _metadata_modified_utc,
  _source_catalog AS _source_catalog,
  chgdat AS Information_Last_Changed_Date,
  chgusr AS Information_Last_Changed_by_User,
  compny AS Company_Identification_Code,
  credat AS Information_Created_Date,
  creusr AS Information_Created_by_User,
  digcod AS Detail_Item_Group_Identification_Code,
  dignam AS Name__slash__Description,
  migcod AS Main_Item_Group_Identification_Code,
  sigcod AS Sub_Item_Group_Identification_Code,
  srtnam AS Short_Name,
  srtnum AS Short_Number,
  txtdsc AS Additional_Text__slash__Description
FROM clockwork_consolidated.rainbow_dig
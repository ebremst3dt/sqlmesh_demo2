MODEL (
  column_descriptions (
    information_last_changed_date = 'Information Last Changed Date',
    information_last_changed_by_user = 'Information Last Changed by User',
    company_identification_code = 'Company Identification Code',
    information_created_date = 'Information Created Date',
    information_created_by_user = 'Information Created by User',
    detail_item_group_identification_code = 'Detail Item Group Identification Code',
    name__slash__description = 'Name / Description',
    main_item_group_identification_code = 'Main Item Group Identification Code',
    sub_item_group_identification_code = 'Sub Item Group Identification Code',
    short_name = 'Short Name',
    short_number = 'Short Number',
    additional_text__slash__description = 'Additional Text / Description'
  )
);

SELECT
  txtdsc AS Additional_Text__slash__Description,
  compny AS Company_Identification_Code,
  digcod AS Detail_Item_Group_Identification_Code,
  credat AS Information_Created_Date,
  creusr AS Information_Created_by_User,
  chgdat AS Information_Last_Changed_Date,
  chgusr AS Information_Last_Changed_by_User,
  migcod AS Main_Item_Group_Identification_Code,
  dignam AS Name__slash__Description,
  srtnam AS Short_Name,
  srtnum AS Short_Number,
  sigcod AS Sub_Item_Group_Identification_Code
FROM clockwork_sllclockdb01_dc_sll_se.rainbow_dig
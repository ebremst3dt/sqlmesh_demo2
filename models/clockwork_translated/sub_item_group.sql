MODEL (
  column_descriptions (
    _data_modified_utc = 'When data was modified at source.',
    _metadata_modified_utc = 'When data was inserted here.',
    _source_catalog = 'Which catalog the source came from.',
    additional_text__slash__description = 'Additional Text / Description',
    asset_type_identification_code = 'Asset Type Identification Code',
    base_unit_identification_code = 'Base Unit Identification Code',
    budget_unit_identification_code = 'Budget Unit Identification Code',
    company_identification_code = 'Company identification code',
    consuming_unit_identification_code = 'Consuming Unit Identification Code',
    cost_center_identification_code = 'Cost Center Identification Code',
    detail_unit_identification_code = 'Detail Unit Identification Code',
    forecast_profile_identification_code = 'Forecast Profile Identification Code',
    individ_group_identification_code = 'Individ Group Identification Code',
    information_created_date = 'Information Created Date',
    information_created_by_user = 'Information created by user',
    information_last_changed_by_user = 'Information last changed by user',
    information_last_changed_date = 'Information last changed date',
    item_category_identification_code = 'Item Category Identification Code',
    item_type_identification_code = 'Item Type Identification Code',
    logistic_group_identification_code = 'Logistic Group Identification Code',
    main_item_group_identification_code = 'Main item group identification code',
    planning_group_identification_code = 'Planning Group Identification Code',
    product_family_identification_code = 'Product Family Identification Code',
    product_group_identification_code = 'Product Group Identification Code',
    product_manager_role_identification_code = 'Product Manager Role Identification Code',
    product_model_identification_code = 'Product Model Identification Code',
    product_planner_role_identification_code = 'Product Planner Role Identification Code',
    product_range_identification_code = 'Product Range Identification Code',
    purchase_budget_account_identification_code = 'Purchase Budget Account Identification Code',
    purchase_unit = 'Purchase Unit',
    purchaser_role_identification_code = 'Purchaser Role Identification Code',
    quality_assurance_model_identification_code = 'Quality Assurance Model Identification Code',
    quality_manager_role = 'Quality Manager Role',
    reinitiate_values_when_changed_on_item = 'Reinitiate Values when Changed on Item',
    relation_ratio_from_budget_unit_to_base_unit = 'Relation Ratio from Budget Unit to Base Unit',
    relation_ratio_from_consuming_unit_to_base_unit = 'Relation Ratio from Consuming Unit to Base Unit',
    relation_ratio_from_detail_unit_to_base_unit = 'Relation Ratio from Detail Unit to Base Unit',
    relation_ratio_from_purchase_unit_to_base_unit = 'Relation Ratio from Purchase Unit to Base Unit',
    relation_ratio_from_sales_unit_to_base_unit = 'Relation Ratio from Sales Unit to Base Unit',
    relation_type_from_budget_unit_to_base_unit = 'Relation Type from Budget Unit to Base Unit',
    relation_type_from_consuming_unit_to_base_unit = 'Relation Type from Consuming Unit to Base Unit',
    relation_type_from_purchase_unit_to_base_unit = 'Relation Type from Purchase Unit to Base Unit',
    relation_type_from_sales_unit_to_base_unit = 'Relation Type from Sales Unit to Base Unit',
    sales_unit_idenfication_code = 'Sales Unit Idenfication Code',
    seller__slash__salesperson_identification_code = 'Seller / Salesperson Identification Code',
    shortname = 'Shortname',
    shortnumber = 'Shortnumber',
    sub_level_required = 'Sub Level Required',
    sub_item_group_identification_code = 'Sub item group identification code',
    sub_item_group_name__slash__description = 'Sub item group name / description',
    tax_group_identification_cocde = 'Tax Group Identification Cocde',
    relation_tyoe_from_detail_unit_to_base_unit = 'relation Tyoe from Detail Unit to Base Unit'
  )
);

SELECT
  _data_modified_utc AS _data_modified_utc,
  _metadata_modified_utc AS _metadata_modified_utc,
  _source_catalog AS _source_catalog,
  astcod AS Asset_Type_Identification_Code,
  basunt AS Base_Unit_Identification_Code,
  budunt AS Budget_Unit_Identification_Code,
  chgdat AS Information_last_changed_date,
  chgusr AS Information_last_changed_by_user,
  cnsunt AS Consuming_Unit_Identification_Code,
  compny AS Company_identification_code,
  credat AS Information_Created_Date,
  creusr AS Information_created_by_user,
  csccod AS Cost_Center_Identification_Code,
  dtlunt AS Detail_Unit_Identification_Code,
  fcpcod AS Forecast_Profile_Identification_Code,
  ictcod AS Item_Category_Identification_Code,
  idgcod AS Individ_Group_Identification_Code,
  itycod AS Item_Type_Identification_Code,
  logcod AS Logistic_Group_Identification_Code,
  migcod AS Main_item_group_identification_code,
  pfmcod AS Product_Family_Identification_Code,
  pgrcod AS Product_Group_Identification_Code,
  plgcod AS Planning_Group_Identification_Code,
  pmdcod AS Product_Model_Identification_Code,
  pmgcod AS Product_Manager_Role_Identification_Code,
  pplcod AS Product_Planner_Role_Identification_Code,
  prbuac AS Purchase_Budget_Account_Identification_Code,
  prccod AS Purchaser_Role_Identification_Code,
  prcunt AS Purchase_Unit,
  prncod AS Product_Range_Identification_Code,
  qamcod AS Quality_Assurance_Model_Identification_Code,
  qlmcod AS Quality_Manager_Role,
  reinit AS Reinitiate_Values_when_Changed_on_Item,
  rrbbud AS Relation_Ratio_from_Budget_Unit_to_Base_Unit,
  rrbcns AS Relation_Ratio_from_Consuming_Unit_to_Base_Unit,
  rrbdtl AS Relation_Ratio_from_Detail_Unit_to_Base_Unit,
  rrbprc AS Relation_Ratio_from_Purchase_Unit_to_Base_Unit,
  rrbsal AS Relation_Ratio_from_Sales_Unit_to_Base_Unit,
  rtbbud AS Relation_Type_from_Budget_Unit_to_Base_Unit,
  rtbcns AS Relation_Type_from_Consuming_Unit_to_Base_Unit,
  rtbdtl AS relation_Tyoe_from_Detail_Unit_to_Base_Unit,
  rtbprc AS Relation_Type_from_Purchase_Unit_to_Base_Unit,
  rtbsal AS Relation_Type_from_Sales_Unit_to_Base_Unit,
  salunt AS Sales_Unit_Idenfication_Code,
  sapcod AS Seller__slash__Salesperson_Identification_Code,
  sigcod AS Sub_item_group_identification_code,
  signam AS Sub_item_group_name__slash__description,
  srtnam AS Shortname,
  srtnum AS Shortnumber,
  sublvl AS Sub_Level_Required,
  txgcod AS Tax_Group_Identification_Cocde,
  txtdsc AS Additional_Text__slash__Description
FROM clockwork_consolidated.rainbow_sig
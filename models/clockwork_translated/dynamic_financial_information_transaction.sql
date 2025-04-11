MODEL (
  column_descriptions (
    _data_modified_utc = 'When data was modified at source.',
    _metadata_modified_utc = 'When data was inserted here.',
    _source_catalog = 'Which catalog the source came from.',
    company_identification_code = 'Company Identification Code',
    data_value_entered_at_date = 'Data Value Entered at Date',
    data_value_entered_by_user = 'Data Value Entered by User',
    data_value_entered_in_program = 'Data Value Entered in Program',
    data_value_entry_form = 'Data Value Entry Form',
    definition_code_according_to_type = 'Definition Code According to Type',
    definition_type = 'Definition Type',
    definition_type_defined_as_account = 'Definition Type defined as Account',
    definition_type_defines_as_dimension = 'Definition Type defines as Dimension',
    dynamic_definition_information_model_code = 'Dynamic Definition Information Model Code',
    information_created_date = 'Information Created Date',
    information_created_by_user = 'Information Created by User',
    information_last_changed_date = 'Information Last Changed Date',
    information_last_changed_by_user = 'Information Last Changed by User',
    model_sequence_number = 'Model Sequence Number',
    sequence_number_ref_to_transaction = 'Sequence Number (Ref to Transaction)',
    tax_liability_handling = 'Tax Liability Handling'
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
  defacc AS Definition_Type_defined_as_Account,
  defcod AS Definition_Code_According_to_Type,
  defdim AS Definition_Type_defines_as_Dimension,
  deftyp AS Definition_Type,
  dficod AS Dynamic_Definition_Information_Model_Code,
  dfiseq AS Model_Sequence_Number,
  entdat AS Data_Value_Entered_at_Date,
  entfrm AS Data_Value_Entry_Form,
  entpgm AS Data_Value_Entered_in_Program,
  entusr AS Data_Value_Entered_by_User,
  seqnum AS Sequence_Number_Ref_to_Transaction,
  taxtyp AS Tax_Liability_Handling
FROM clockwork_consolidated.rainbow_dfitrn
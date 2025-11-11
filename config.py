
# configuration values for connecting to the database
driver_32 = 'ODBC Driver 17 for SQL Server'
driver_64 = 'SQL Server'
test_server = 'DoverVTest'
live_server = 'DoverV'
msa_driver = 'Microsoft Access Driver (*.mdb, *.accdb)'

user=''
pwd=''

delivery_process = 'N:\CancerEpidem\BrBreakthrough\DeliveryProcess'

schema_derivation = delivery_process + r'\Schema_and_Derivation_utils\\Questionnaire\\R0'

Delivery_log_path = delivery_process + r'\Logs'

json_path = schema_derivation + r'\json_schemas'

r0_json_path = json_path + r'\raw'

r0_json_path_pii = json_path + r'\post_pii'

out_json_path = delivery_process + r'\Data_Output_Testing'

mock_data_path = schema_derivation + r'\mock_data'

mock_data_df_path = schema_derivation + r'\mock_data\\dataFrame'

mock_data_json_path = schema_derivation + r'\mock_data\\json'

validation_path = schema_derivation + r'\validation'

ct_path = validation_path + r'\_change_tracking'
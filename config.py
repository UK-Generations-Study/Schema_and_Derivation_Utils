# configuration values for connecting to the database
driver_32 = 'ODBC Driver 17 for SQL Server'
driver_64 = 'SQL Server'
test_server = 'DoverVTest'
live_server = 'DoverV'
msa_driver = 'Microsoft Access Driver (*.mdb, *.accdb)'

user=''
pwd=''

casum_json_path = 'N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\json_schemas'

canreg_data_path = 'N:\\NOBACKUP\DARS-NIC-656751-H5K0Y-V1_Received_20250114\\unzipped'

canreg_fileanme = 'Table_3.csv'

casum_report_path = "N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary"

casum_ICD_conversion_file = "ICD9_to_10.csv"

casum_StudyID = ['studyid', 'Studyid', 'studyID', 'STUDYID', 'StudyId']

casum_convert_fields = {"StudyID":"int", "PersonId":"int",\
                        "BASISOFDIAGNOSIS": "str", "BEHAVIOUR_ICD10_O2": "str", "CODING_SYSTEM": "str", "BEHAVIOUR_CODED": "str",\
                        "diagdate": "date", "repdate": "date", 'STUDY_ID': 'int'}

#casum_clean_null_fields = {"Stage_Other_Not_Reported": "N", "Stage_Best": "N", \
#                           "Stage_UICC": "N", "Stage_FIGO": "N", "Grade_HL": "N", \
#                           "Grade_I_II_III": "N"}
    
casum_canreg_dtype_updates = {"EXCISIONMARGIN" : "str", "CLARKS": "str", "ER_SCORE": "str", "PR_SCORE":"str",\
                              "SCREENINGSTATUSFULL_CODE":"str", "BRESLOW": "str"}

casum_data_sources = {'FlaggingCancers': 'FlaggingCancers.json',
#                      'FlaggingDeaths': 'FlaggingDeaths.json',
                      'Histopath_BrCa_GS_v1': 'HistoPath_BrCa.json',
                      'OvCa_Histopath_II': 'HistoPath_OvCa.json', 
                      'CancerRegistry': 'CancerRegistry.json',
                      'casummary_v1': 'CaSummary.json',
                      'NewCancerSummary': 'NewCanSummary.json'}

brca_variables_to_map = {"ER_Status": "ER_STATUS",
                         "InvasiveGrade": "GRADE",
                         "PR_Status": "PR_STATUS",
                         "ScreenDetected": "SCREEN_DETECTED",
                         "Side": "LATERALITY",
                         "HER2_Status": "HER2_STATUS",
                         "DCISGrade": "GRADE"}

brca_special_rules = {"ER_Status": {"NP": "X"},
                      "InvasiveGrade": {"NA": "GX", "1": "G1", "2": "G2", "3": "G3"},
                      "PR_Status": {"NP": "X"},
                      "ScreenDetected": {"NK": "9"},
                      "Side": {"U": "9"},
                      "HER2_Status": {"NP": "X"},
                      "DCISGrade": {"L":"GL", "I":"GI", "IL":"GI", "H":"GH", "HI": "GH", "N": "GX"}}

ovca_variables_to_map = {"Grade_I_II_III": "GRADE",
                         "Stage_FIGO": "STAGE"}

ovca_special_rules = {"Grade_I_II_III": {"N": "G4", "1": "G1", "2": "G2", "3": "G3"},
                      "Stage_FIGO": {"1": "Stage 1", "2": "Stage 2", "3": "Stage 3", "4": "Stage 4"}}

legacy_variables_to_map = {"er_Status": "ER_STATUS",
                           "pr_Status": "PR_STATUS",
                           "her2_Status": "HER2_STATUS",
                           "side": "LATERALITY",
                           "Screen_Detected": "SCREEN_DETECTED"}

legacy_special_rules ={"er_Status": {"Positive": "P", "Negative": "N"},
                       "pr_Status": {"Positive": "P", "Negative": "N"},
                       "Screen_Detected": {"U": "9", "X": "8"},
                       "side": {"1": "L", "2": "R", "3": "B", "4": "M", None:"9"},
                       "her2_Status": {"Positive": "P", "Negative": "N", "Borderline": "B"},}

stage_rules = {
                'StagePattern': ['T1a','T1b','T1c','T2a','T2b','T3','T4','M1a','M1b','M1c'],
                'N0': ['1A','1A','1A','1B','2A','2B','3A','4A','4A','4B'],
                'N1': ['2A','2A','2A','2B','2B','3A','3A','4A','4A','4B'],
                'N2a':['2B','2B','2B','3A','3A','3A','3B','4A','4A','4B'],
                'N2b':['3A','3A','3A','3B','3B','3B','3B','4A','4A','4B'],
                'N3': ['3B','3B','3B','3B','3B','3C','3C','4A','4A','4B']
            }

dateList = [[51, 52, 53], [135, 136, 137], [143, 144, 145], [151, 152, 153], [475, 476], [477, 478], [485, 486], [487, 488], [495, 496], [497, 498], [514, 515], [516, 517], [518, 519], 
[536, 537], [538, 539], [540, 541], [562, 563, 564], [575, 576, 577], [588, 589, 590], [601, 602, 603], [614, 615, 616], [627, 628, 629], [640, 641, 642], [653, 654, 655], [666, 667, 668], 
[679, 680, 681], [692, 693, 694], [705, 706, 707], [718, 719, 720], [858, 859], [860, 861], [867, 868], [869, 870], [876, 877], [878, 879], [885, 886], [887, 888], [894, 895], [896, 897], 
[903, 904], [905, 906], [912, 913], [914, 915], [920, 921], [922, 923], [928, 929], [930, 931], [937, 938], [939, 940], [945, 946], [947, 948], [953, 954], [955, 956], [961, 962], [963, 964], 
[969, 970], [971, 972], [978, 979], [980, 981], [986, 987], [988, 989], [994, 995], [996, 997], [1002, 1003], [1004, 1005], [1010, 1011], [1012, 1013], [1107], [1123, 1124], [1127, 1128], 
[1129, 1130], [1144, 1145], [1148, 1149], [1150, 1151], [1165, 1166], [1169, 1170], [1171, 1172], [1194, 1195], [1196, 1197], [1204, 1205], [1206, 1207], [1209, 1210], [1211, 1212], [1217, 1218], 
[1219, 1220], [1225, 1226], [1227, 1228], [1233, 1234], [1235, 1236], [1241, 1242], [1243, 1244], [1249, 1250], [1256, 1257], [1258, 1259], [1265, 1266], [1267, 1268], [1271, 1272], [1273, 1274], 
[1280, 1281], [1288, 1289], [1290, 1291], [1297, 1298], [1299, 1300], [1303, 1304], [1305, 1306], [1321, 1322], [1337, 1338], [1353, 1354], [1401], [1409], [1418], [1449], [1477, 1478], 
[1481, 1482], [1489, 1490], [1491, 1492], [1493, 1494], [1516], [1519, 1520], [1527, 1528], [1529, 1530], [1531, 1532], [1626, 1627], [1628, 1629], [1635, 1636], [1637, 1638], [1644, 1645], 
[1646, 1647], [1664, 1665], [1666, 1667], [1673, 1674], [1675, 1676], [1682, 1683], [1684, 1685], [1705, 1706], [1707, 1708], [1714, 1715], [1716, 1717], [1723, 1724], [1725, 1726], [1819], 
[1824], [1829], [1836], [1843, 1844], [1845, 1846], [1849], [1856, 1857], [1858, 1859], [1862], [1869, 1870], [1871, 1872], [1875, 1876], [1877, 1878], [1879, 1880], [1888, 1889], [1890, 1891], 
[1892, 1893], [1901, 1902], [1903, 1904], [1905, 1906], [1914, 1915], [1916, 1917], [1918, 1919], [1927, 1928], [1929, 1930], [1931, 1932], [1940, 1941], [1942, 1943], [1944, 1945], [2051], 
[2052], [2061], [2062], [2066], [2067], [2072], [2073], [2077], [2078], [2082], [2083], [2087], [2088], [2092], [2093], [2421], [2422], [2503, 2504, 2505], [2514], [2516], [2519, 2520, 2521], 
[2527, 2528, 2529], [2538], [2540], [2543, 2544, 2545], [2566, 2567, 2568], [2572], [2576, 2577, 2578], [2582], [2586, 2587, 2588], [2592], [2596, 2597, 2598], [2602], [2606, 2607, 2608], 
[2612], [2617, 2618, 2619], [2623], [2627, 2628, 2629], [2633], [2637, 2638, 2639], [2643], [2647, 2648, 2649], [2653], [2657, 2658, 2659], [2663], [2667, 2668, 2669], [2673], [2677, 2678, 2679], 
[2683], [2687, 2688, 2689], [2693], [2697, 2698, 2699], [2702], [2706, 2707, 2708], [2711], [2714, 2715, 2716], [2719], [3001, 3002, 3003]]

newQuestionDict = {
    51: "DOB",
    135: "RecordedHeight_1",
    143: "RecordedHeight_2",
    151: "RecordedHeight_3",
    475: "TemporaryPeriodStop_Start_2",
    477: "TemporaryPeriodStop_End_2",
    485: "TemporaryPeriodStop_Start_3",
    487: "TemporaryPeriodStop_End_3",
    495: "TemporaryPeriodStop_Start_4",
    497: "TemporaryPeriodStop_End_4",
    514: "OvaryOperation_3",
    516: "OvaryOperation_3_RangeStart",
    518: "OvaryOperation_3_RangeEnd",
    536: "OvaryOperation_4",
    538: "OvaryOperation_4_RangeStart",
    540: "OvaryOperation_4_RangeEnd",
    562: "PregnancyEndDate_1",
    575: "PregnancyEndDate_2",
    588: "PregnancyEndDate_3",
    601: "PregnancyEndDate_4",
    614: "PregnancyEndDate_5",
    627: "PregnancyEndDate_6",
    640: "PregnancyEndDate_7",
    653: "PregnancyEndDate_8",
    666: "PregnancyEndDate_9",
    679: "PregnancyEndDate_10",
    692: "PregnancyEndDate_11",
    705: "PregnancyEndDate_12",
    718: "PregnancyEndDate_13",
    858: "ContraceptivePill_4_Start",
    860: "ContraceptivePill_4_End",
    867: "ContraceptivePill_5_Start",
    869: "ContraceptivePill_5_End",
    876: "ContraceptivePill_6_Start",
    878: "ContraceptivePill_6_End",
    885: "ContraceptivePill_7_Start",
    887: "ContraceptivePill_7_End",
    894: "ContraceptivePill_8_Start",
    896: "ContraceptivePill_8_End",
    903: "ContraceptivePill_9_Start",
    905: "ContraceptivePill_9_End",
    912: "ContraceptiveInjected_3_Start",
    914: "ContraceptiveInjected_3_End",
    920: "ContraceptiveInjected_4_Start",
    922: "ContraceptiveInjected_4_End",
    928: "ContraceptiveInjected_5_Start",
    930: "ContraceptiveInjected_5_End",
    937: "HormonePreparation_4_Start",
    939: "HormonePreparation_4_End",
    945: "HormonePreparation_5_Start",
    947: "HormonePreparation_5_End",
    953: "HormonePreparation_6_Start",
    955: "HormonePreparation_6_End",
    961: "HormonePreparation_7_Start",
    963: "HormonePreparation_7_End",
    969: "HormonePreparation_8_Start",
    971: "HormonePreparation_8_End",
    978: "OtherHormone_3_Start",
    980: "OtherHormone_3_End",
    986: "OtherHormone_4_Start",
    988: "OtherHormone_4_End",
    994: "OtherHormone_5_Start",
    996: "OtherHormone_5_End",
    1002: "OtherHormone_6_Start",
    1004: "OtherHormone_6_End",
    1010: "OtherHormone_7_Start",
    1012: "OtherHormone_7_End",
    1107: "MammogramYear",
    1123: "BenignBreastDiagnosis_2",
    1127: "BenignBreastDiagnosis_2_RangeStart",
    1129: "BenignBreastDiagnosis_2_RangeEnd",
    1144: "BenignBreastDiagnosis_3",
    1148: "BenignBreastDiagnosis_3_RangeStart",
    1150: "BenignBreastDiagnosis_3_RangeEnd",
    1165: "BenignBreastDiagnosis_4",
    1169: "BenignBreastDiagnosis_4_RangeStart",
    1171: "BenignBreastDiagnosis_4_RangeEnd",
    1194: "Radiotherapy_1_Start",
    1196: "Radiotherapy_1_End",
    1204: "DrugTherapy_1_Start",
    1206: "DrugTherapy_1_End",
    1209: "DrugTherapy_2_Start",
    1211: "DrugTherapy_2_End",
    1217: "DrugTherapy_3_Start",
    1219: "DrugTherapy_3_End",
    1225: "DrugTherapy_4_Start",
    1227: "DrugTherapy_4_End",
    1233: "DrugTherapy_5_Start",
    1235: "DrugTherapy_5_End",
    1241: "DrugTherapy_6_Start",
    1243: "DrugTherapy_6_End",
    1249: "BreastCancerDiagnosis_2",
    1256: "Radiotherapy_2_Start",
    1258: "Radiotherapy_2_End",
    1265: "DrugTherapy_2ndCancer_Start",
    1267: "DrugTherapy_2ndCancer_End",
    1271: "DrugTherapy_2ndCancer_2_Start",
    1273: "DrugTherapy_2ndCancer_2_End",
    1280: "BreastCancerDiagnosis_3",
    1288: "Radiotherapy_3_Start",
    1290: "Radiotherapy_3_End",
    1297: "DrugTherapy_3rdCancer_Start",
    1299: "DrugTherapy_3rdCancer_End",
    1303: "DrugTherapy_3rdCancer_2_Start",
    1305: "DrugTherapy_3rdCancer_2_End",
    1321: "BreastSurgery_2",
    1337: "BreastSurgery_3",
    1353: "BreastSurgery_4",
    1401: "OtherCancerYear_1",
    1409: "OtherCancerYear_2",
    1418: "OtherCancerYear_3",
    1449: "HipFractureYear",
    1477: "EatingDisorderDoctor_2",
    1481: "EatingDisorderPeriods_2",
    1489: "EatingDisorder_2",
    1491: "EatingDisorder_2_RangeStart",
    1493: "EatingDisorder_2_RangeEnd",
    1516: "EatingDisorderDoctor_3",
    1519: "EatingDisorderPeriods_3",
    1527: "EatingDisorder_3",
    1529: "EatingDisorder_3_RangeStart",
    1531: "EatingDisorder_3_RangeEnd",
    1626: "Aspirin_3_Start",
    1628: "Aspirin_3_End",
    1635: "Aspirin_4_Start",
    1637: "Aspirin_4_End",
    1644: "Aspirin_5_Start",
    1646: "Aspirin_5_End",
    1664: "Ibuprofen_3_Start",
    1666: "Ibuprofen_3_End",
    1673: "Ibuprofen_4_Start",
    1675: "Ibuprofen_4_End",
    1682: "Ibuprofen_5_Start",
    1684: "Ibuprofen_5_End",
    1705: "Painkillers_3_Start",
    1707: "Painkillers_3_End",
    1714: "Painkillers_4_Start",
    1716: "Painkillers_4_End",
    1723: "Painkillers_5_Start",
    1725: "Painkillers_5_End",
    1819: "XRayYear_1",
    1824: "XRayYear_2",
    1829: "XRayYear_3",
    1836: "XRayYear_4",
    1843: "XRay_4_RangeStart",
    1845: "XRay_4_RangeEnd",
    1849: "XRayYear_5",
    1856: "XRay_5_RangeStart",
    1858: "XRay_5_RangeEnd",
    1862: "XRayYear_6",
    1869: "XRay_6_RangeStart",
    1871: "XRay_6_RangeEnd",
    1875: "XRay_7",
    1877: "XRay_7_RangeStart",
    1879: "XRay_7_RangeEnd",
    1888: "XRay_8",
    1890: "XRay_8_RangeStart",
    1892: "XRay_8_RangeEnd",
    1901: "XRay_9",
    1903: "XRay_9_RangeStart",
    1905: "XRay_9_RangeEnd",
    1914: "XRay_10",
    1916: "XRay_10_RangeStart",
    1918: "XRay_10_RangeEnd",
    1927: "XRay_11",
    1929: "XRay_11_RangeStart",
    1931: "XRay_11_RangeEnd",
    1940: "XRay_12",
    1942: "XRay_12_RangeStart",
    1944: "XRay_12_RangeEnd",
    2051: "JobStartYear_1",
    2052: "JobEndYear_1",
    2061: "NightWorkStart_1",
    2062: "NightWorkEnd_1",
    2066: "NightWorkStart_2",
    2067: "NightWorkEnd_2",
    2072: "NightWorkStart_3",
    2073: "NightWorkEnd_3",
    2077: "NightWorkStart_4",
    2078: "NightWorkEnd_4",
    2082: "NightWorkStart_5",
    2083: "NightWorkEnd_5",
    2087: "NightWorkStart_6",
    2088: "NightWorkEnd_6",
    2092: "NightWorkStart_7",
    2093: "NightWorkEnd_7",
    2421: "AircrewTravel_Start",
    2422: "AircrewTravel_End",
    2503: "FatherDOB",
    2514: "FatherCancerYear_1",
    2516: "FatherCancerYear_2",
    2519: "FatherDeathDate",
    2527: "MotherDOB",
    2538: "MotherCancerYear_1",
    2540: "MotherCancerYear_2",
    2543: "MotherDeathDate",
    2566: "Sibling1DOB",
    2572: "Sibling1CancerYear",
    2576: "Sibling2DOB",
    2582: "Sibling2CancerYear",
    2586: "Sibling3DOB",
    2592: "Sibling3CancerYear",
    2596: "Sibling4DOB",
    2602: "Sibling4CancerYear",
    2606: "Sibling5DOB",
    2612: "Sibling5CancerYear",
    2617: "Sibling6DOB",
    2623: "Sibling6CancerYear",
    2627: "Sibling7DOB",
    2633: "Sibling7CancerYear",
    2637: "Sibling8DOB",
    2643: "Sibling8CancerYear",
    2647: "Sibling9DOB",
    2653: "Sibling9CancerYear",
    2657: "Sibling10DOB",
    2663: "Sibling10CancerYear",
    2667: "Sibling11DOB",
    2673: "Sibling11CancerYear",
    2677: "Sibling12DOB",
    2683: "Sibling12CancerYear",
    2687: "Sibling13DOB",
    2693: "Sibling13CancerYear",
    2697: "ChildCancerDOB_1",
    2702: "ChildCancerYear_1",
    2706: "ChildCancerDOB_2",
    2711: "ChildCancerYear_2",
    2714: "ChildCancerDOB_3",
    2719: "ChildCancerYear_3",
    3001: "QuestionnaireCompletionDate"
}

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
DATA.PY
------------------------------------------------------------------------------
import pandas as pd
from queries import *
from datetime import datetime
from secrets import token_hex


job_key = token_hex(16)
job_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


start_job_tracking(job_key,job_start)



xwalk = pd.read_sql_query(xwalk_q,db_engine_confidential)
xwalk_practitioner = xwalk[(xwalk['RecordType']=='Practitioner')]
xwalk_group = xwalk[(xwalk['RecordType']=='Group')]

active_prov =pd.read_sql_query(active_prov_q,db_engine_confidential)
term_prov = pd.read_sql_query(term_prov_q,db_engine_confidential)

prac_cols =[0,1,2,3,7,8,10,11,12]
active_prov_prac = active_prov[active_prov.columns[prac_cols]]
term_prov_prac = term_prov[term_prov.columns[prac_cols]]
# print(xwalk_practitioner.index)
# print(active_prov_prac.index)
# index must be the same to merge. doing the index reset below to make both df's range index vs range and int64
xwalk_practitioner.reset_index(inplace=True)

active_prov_prac_final =pd.merge(active_prov_prac,
                    xwalk_practitioner[['PROVIDER_TYPE_CD','confidential']],
                    how='left',
                    on='PROVIDER_TYPE_CD')

term_prov_prac_final =pd.merge(term_prov_prac,
                    xwalk_practitioner[['PROVIDER_TYPE_CD','confidential']],
                    how='left',
                    on='PROVIDER_TYPE_CD')

group_cols = [0,4,5,6,8,7,9,10,11,12]
active_prov_group = active_prov[active_prov.columns[group_cols]]
active_prov_group = active_prov_group.drop_duplicates()
term_prov_group = term_prov[term_prov.columns[group_cols]]
term_prov_group = term_prov_group.drop_duplicates()

xwalk_group.reset_index(inplace=True)
active_prov_group.reset_index(inplace=True)
term_prov_group.reset_index(inplace=True)

active_prov_group_final =pd.merge(active_prov_group,
                    xwalk_group[['PROVIDER_TYPE_CD','confidential']],
                    how='left',
                    left_on='GROUP_TYPE',
                    right_on='PROVIDER_TYPE_CD')
active_prov_group_final.drop(['index','PROVIDER_TYPE_CD'],axis=1,inplace=True)

term_prov_group_final = pd.merge(term_prov_group,
                    xwalk_group[['PROVIDER_TYPE_CD','confidential']],
                    how='left',
                    left_on='GROUP_TYPE',
                    right_on='PROVIDER_TYPE_CD')
term_prov_group_final.drop(['index','PROVIDER_TYPE_CD'],axis=1,inplace=True)

group_final = pd.concat([active_prov_group_final,term_prov_group_final])
prac_final = pd.concat([active_prov_prac_final,term_prov_prac_final])

group_final = group_final[['confidential']]

prac_final = prac_final[['confidential']]

#delete records that exist today. This is useful if we need to run twice for some reason
result=db_engine_confidential.execute(delete_query_group)
result=db_engine_confidential.execute(delete_query_prac)

transaction_dts = datetime.now()
group_final['TRANSACTION_DTS']=transaction_dts
group_final = group_final[~group_final.GROUP_TAX_ID.str.contains("N/A")]
group_record_count = len(group_final.index)
group_final.to_sql('PROVIDER_TRACKING_GROUP',db_engine_confidential,index=False,if_exists='append')

prac_final['TRANSACTION_DTS']=transaction_dts
prac_record_count = len(prac_final.index)
prac_final.to_sql('PROVIDER_TRACKING_PRACTITIONER',db_engine_confidential,index=False,if_exists='append')

read_record_count = prac_record_count+group_record_count

read_record_count_job(job_key,read_record_count)

result = db_engine_confidential.execute(get_server_written_record_count)
result = result.first()
server_record_count = result.Record_Count

wrote_record_count_job(job_key,server_record_count)


file_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')[:-3]
export_group_report= pd.read_sql_query(get_group_report,db_engine_confidential)
export_group_report.to_excel(r'C:\Users\y14082\OneDrive - healthnow.org\Projects\ActiveTermedProvidersFileFeed\sent_files\PROVIDER_GROUP_TRACKING_'+str(file_timestamp)+'.xlsx',header=True, index = False)

export_prac_report= pd.read_sql_query(get_prac_report,db_engine_confidential)
export_prac_report.to_excel(r'C:\Users\y14082\OneDrive - healthnow.org\Projects\ActiveTermedProvidersFileFeed\sent_files\PROVIDER_PRACTITIONER_TRACKING_'+str(file_timestamp)+'.xlsx',header=True, index = False)


job_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
complete_job_tracking(job_key,job_end)

print("Job Complete")
# prac cols
# RECORD_TYPE 0
# PROVIDER_NM 1
# PRACTITIONER_NPI 2
# PROVIDER_TYPE_CD 3
# FACETS_ID_TYPE 8
# PROVIDER_ID 10
# PROVIDER_NTWK_REL_EFF_DT 11
# PROVIDER_NTWK_REL_TERM_DT 12


# group cols
# RECORD_TYPE 0
# PROVIDER_GROUP_NM 4
# PROVIDER_GROUP_NPI 5
# GROUP_TAX_ID 6
# FACETS_ID_TYPE 8
# PROVIDER_GROUP_PROVIDER_ID 7
# GROUP_TYPE 9



------------------------------------------------------------------------------
QUERIES.PY 
------------------------------------------------------------------------------
from datetime import date, timedelta
from sqlalchemy import create_engine,text



aconfidential_conn = ('mssql+pyodbc://<Server>/<db>?driver=SQL+Server')
db_engine_confidential = create_engine(confidential_conn)
confidential_conn = ('mssql+pyodbc://<Server>/<db>?driver=SQL+Server')
db_engine_confidential = create_engine(confidential_conn)
confidentialconn = ('mssql+pyodbc://<Server>/<db>?driver=SQL+Server')
db_engine_confidential = create_engine(confidential_conn)


core_date = date.today()
one_day = timedelta(1)
start_date = core_date-one_day
yesterday = start_date-one_day
tomorrow = start_date+one_day

xwalk_q = """confidential"""

active_prov_q ="""confidential"""

term_prov_q ="""confidential"""

delete_query_prac = """confidential"""

delete_query_group = """confidential"""

get_prac_report = """confidential"""

get_group_report = """confidential"""

get_server_written_record_count = """confidential"""




# data pipeline job tracking log
def start_job_tracking(input_job_key,input_job_start):
	connection = db_engine_adl.raw_connection()
	query = """EXEC <sp_name> @PIPELINE_JOB_KEY = '"""+str(input_job_key)+"""',
						@JOB_NAME = 'Track active term providers',@START_DTS='"""+str(input_job_start)+"""',@PIPELINE_SOURCE='Python'"""
	try:
		cursor = connection.cursor()
		cursor.execute(query)
		cursor.close()
		connection.commit()
	finally:
		connection.close()

def read_record_count_job(input_job_key,record_count):
	connection = db_engine_adl.raw_connection()
	query = """exec <sp_name> @PIPELINE_JOB_KEY = '"""+str(input_job_key)+"""',@RECORDS_READ="""+str(record_count)
	try:
		cursor = connection.cursor()
		cursor.execute(query)
		cursor.close()
		connection.commit()
	finally:
		connection.close()

def wrote_record_count_job(input_job_key,record_count):
	connection = db_engine_adl.raw_connection()
	query = """exec <sp_name> @PIPELINE_JOB_KEY = '"""+str(input_job_key)+"""',@RECORDS_WROTE="""+str(record_count)
	try:
		cursor = connection.cursor()
		cursor.execute(query)
		cursor.close()
		connection.commit()
	finally:
		connection.close()

def complete_job_tracking(input_job_key,input_job_end):
	connection = db_engine_adl.raw_connection()
	query = """exec <sp_name> @PIPELINE_JOB_KEY = '"""+str(input_job_key)+"""',@JOB_MESSAGE='Complete',@END_DTS='"""+str(input_job_end)+"""'"""
	try:
		cursor = connection.cursor()
		cursor.execute(query)
		cursor.close()
		connection.commit()
	finally:
		connection.close()

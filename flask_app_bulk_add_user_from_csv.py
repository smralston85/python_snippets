from confidential.models import HUB_USER,HUB_COMPANY,SAT_USER,HUB_PATIENT_ORDER as HPO
from confidential.db_operations.dbtranconn import Session,transaction_engine
from sqlalchemy import text
from flask import flash
from confidential import app,db
import glob
import os
import pandas as pd
from datetime import datetime

def process_addusers():
    list_of_files = glob.glob(os.path.join(app.root_path, 'static/files', '*.csv'))
    max_file = max(list_of_files,key=os.path.getctime)
    dataset = pd.read_csv(max_file)
    df=pd.DataFrame(dataset)


    ###############################USER PROCESSING####################################
    # Separate user from company
    usr_col =[0,1,2,3,4,5,6,7,8,9]
    df_usr = df[df.columns[usr_col]]
    df_usr.columns = ['USR_ID','SAT_USR_FNAME', 'SAT_USR_LNAME', 'SAT_USR_ADDR1', 'SAT_USR_ADDR2', 'SAT_USR_CITY', 'SAT_USR_STATE', 'SAT_USR_ZIP', 'SAT_USR_EMAIL', 'SAT_USR_PHONE']
    df_usr = df_usr.drop_duplicates()
    # create list of all users that are in the data frame set removes duplicates from the list but doesnt hold original order of items in the list
    usr_id_list = list(set(df_usr['USR_ID']))
    # find if the user exists in the db already 
    usr_record = db.session.query(HUB_USER.USR_ID).filter(HUB_USER.USR_ID.in_(usr_id_list)).all()
    # create list of usrs that already exist in db
    usr_id_list = [usr_id for usr_id, in usr_record]
    # remove all records from data frame where usr already exists in db 
    df_usr = df_usr[~df_usr['USR_ID'].isin(usr_id_list)]

    # if email exists in db already stop opertations
    usr_email_list = list(set(df_usr['SAT_USR_EMAIL']))
    usr_email_record = db.session.query(SAT_USER.SAT_USR_EMAIL).filter(SAT_USER.SAT_USR_EMAIL.in_(usr_email_list)).all()
    db_usr_email_list = [usr_email for usr_email, in usr_email_record]
    if db_usr_email_list:
        usr_email_list = [x for x in usr_email_list if x in db_usr_email_list]
    usr_email_list.sort()
    db_usr_email_list.sort()
    # if email exists in db already stop opertations

    # if email exists as duplicate in file stop operations
    usr_email_list_file_dedupe = list(set(df_usr['SAT_USR_EMAIL']))
    usr_email_list_file = list(df_usr['SAT_USR_EMAIL'])
    usr_email_list_file_dedupe.sort()
    usr_email_list_file.sort()
    # if email exists as duplicate in file stop operations

    if df_usr.empty:
        flash("All users already exist in the database!")
    elif usr_email_list == db_usr_email_list:
        flash("A user in your file has a matching email of an existing user in the system! Please see: "+str(db_usr_email_list)+ ". Operation stopped!")
        # for debug only
        # flash(usr_email_list)
        # flash(db_usr_email_list)
    elif usr_email_list_file_dedupe != usr_email_list_file:
        flash("There are duplicate emails in your file please correct and start again")
        # for debug only    
        # flash(usr_email_list_file_dedupe)
        # flash(usr_email_list_file)
    else:
        usr_session = Session()
        hub_usr_col =[0]
        df_hub_usr = df_usr[df_usr.columns[hub_usr_col]]
        df_hub_usr = df_hub_usr.drop_duplicates()
        df_hub_usr.to_sql('HUB_USER',db.engine, index=False,if_exists='append')

        sat_usr_col = [0,1,2,3,4,5,6,7,8,9]
        df_sat_usr = df_usr[df_usr.columns[sat_usr_col]]
        df_sat_usr = df_sat_usr.drop_duplicates()

        df_sat_usr.insert(0, "USR_SEQ_ID", "", True)
        usr_name_record = usr_session.query(HUB_USER.USR_SEQ_ID,HUB_USER.USR_ID).all()

        for record in usr_name_record:
            df_sat_usr.loc[df_sat_usr.USR_ID == record.USR_ID, "USR_SEQ_ID"] = record.USR_SEQ_ID

        del df_sat_usr["USR_ID"]

        #if email already exists in sat_user stop operations
        usr_email_record = usr_session.query(SAT_USER.SAT_USR_EMAIL).all()
        usr_email_list = [usr_email for usr_email, in usr_email_record]
        
        df_sat_usr.to_sql('SAT_USER',db.engine, index=False,if_exists='append')
        usr_session.close()
        flash("You have succesfully added new users into the system!")

        
    ###############################USER PROCESSING####################################



    ###############################COPMANY PROCESSING####################################
    # Separate company from user
    comp_col =[10,11,12,13,14,15,16,17]
    df_comp = df[df.columns[comp_col]]
    df_comp.columns = ['COMP_NAME','SAT_COMP_ADDR1','SAT_COMP_ADDR2','SAT_COMP_CITY','SAT_COMP_STATE','SAT_COMP_ZIP','SAT_COMP_EMAIL','SAT_COMP_PHONE']
    df_comp = df_comp.drop_duplicates()
    # create list of all companies that are in the data frame set removes duplicates from the list but doesnt hold original order of items in the list
    company_name_list = list(set(df_comp['COMP_NAME']))
    # find if the company exists in the db already 
    company_record = db.session.query(HUB_COMPANY.COMP_NAME).filter(HUB_COMPANY.COMP_NAME.in_(company_name_list)).all()
    # create list of companies that already exist in db
    comp_nm_list = [comp_nm for comp_nm, in company_record]
    # remove all records from data frame where company already exists in db 
    df_comp = df_comp[~df_comp['COMP_NAME'].isin(comp_nm_list)]


    # add new companies to the database
    if df_comp.empty:
        flash("Copmany "+str(comp_nm_list)+" already exists in the database!")
    else:
        comp_session = Session()
        hub_comp_col =[0]
        df_hub_comp = df_comp[df_comp.columns[hub_comp_col]]
        df_hub_comp = df_hub_comp.drop_duplicates()
        df_hub_comp.to_sql('HUB_COMPANY',db.engine, index=False,if_exists='append')

        sat_comp_col = [0,1,2,3,4,5,6,7]
        df_sat_comp = df_comp[df_comp.columns[sat_comp_col]]
        df_sat_comp = df_sat_comp.drop_duplicates()

        df_sat_comp.insert(0, "COMP_SEQ_ID", "", True)
        company_name_record = comp_session.query(HUB_COMPANY.COMP_SEQ_ID,HUB_COMPANY.COMP_NAME).all()

        for record in company_name_record:
            df_sat_comp.loc[df_sat_comp.COMP_NAME == record.COMP_NAME, "COMP_SEQ_ID"] = record.COMP_SEQ_ID

        del df_sat_comp["COMP_NAME"]
        df_sat_comp.to_sql('SAT_COMPANY',db.engine, index=False,if_exists='append')
        comp_session.close()
        flash("You have succesfully loaded a new company to the system!")
    ###############################COPMANY PROCESSING####################################

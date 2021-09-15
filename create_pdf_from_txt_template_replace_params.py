from locale import format_string
from confidential import app,db,mail
from confidential.models import VIEW_PATIENT_ORDER_DTLS as PD
from fpdf import FPDF
from datetime import datetime
import os



def create_pdf_order(order_id):
    #define local function vars
    variables decalred here. currently confidential

    replace_values = [ ('#request_date#', dt_now_str), ('#patient_name#', patient_name), ("#patient_dob#",patient_dob),("#patient_gender#",patient_gender),
    ("#patient_insurance#",patient_insurance),("#patient_insurance_id#",patient_insurance_id),("#physician_name#",physician_name),("#physician_npi#",physician_npi),
    ("#physician_company#",physician_company), ("#physician_address#",physician_address), ("#physician_name#",physician_name), ("#rendering_provider_name#",rendering_provider_name),
    ("#rendering_provider_phone#",rendering_provider_phone), ("#rendering_provider_address#",rendering_provider_address), ("#rendering_provider_npi#",rendering_provider_npi),
    ("#clinical_test_ordered#",clinical_test_ordered),("#clinical_test_reason#",clinical_test_reason),
    ("#clinical_test_urgent#",clinical_test_urgent), ("#clinical_support_doc_one#",clinical_support_doc_one),
    ("#clinical_support_doc_two#",clinical_support_doc_two),("#physician_title#",physician_title)]

    #create pdf object
    pdf = FPDF()
    #add page to pdf object
    pdf.add_page()
    #set font to pdf object
    pdf.set_font("Arial",size=10)
    #start reading txt template text lines
    file = open(os.path.join(app.root_path, 'file_operations', 'template.txt'),"r")
    

    for line in file:
        #make line a string
        line_string = str(line)
        #identify section headers to change font to bold
        section_label = line_string[0:7]
        #find and replace parameters within txt data
        for k, v in replace_values:
            line_string=line_string.replace(k,v)
        #print out line into pdf
        if section_label == "Section":
            pdf.set_font("Arial",style="B",size=12)
            pdf.cell(250,5,txt=line_string,ln=1,align='l')
        else:
            pdf.set_font("Arial",size=10)
            pdf.cell(250,5,txt=line_string,ln=1,align='l')

    dt_now_pdf = datetime.now()
    dt_now_pdf_str = dt_now_pdf.strftime("%Y%m%d_%H_%M_%S")
    pdf_file_name = "confidential_file_name"+str(order_id)+".pdf"
    pdf.output(os.path.join(app.root_path, 'static/orders_sent', pdf_file_name))

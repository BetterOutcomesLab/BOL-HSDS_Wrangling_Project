import pandas as pd
import numpy as np
from datetime import datetime, date

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/MOH/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/MOH/"

outcome_file_name = "MOH_HospitalAdmissionAll_person_FY_indicator"

#set the start and end for the range of financial years
financial_year_start = '1011'
financial_year_end = '1819'

def read_hospital_admission_data():

   df_admission = pd.read_csv(inputDir + 'prj_2018_29_4_ap_sensetive.csv',usecols=['ppn', 'episode_start_date','episode_to_date'],parse_dates=['episode_start_date','episode_to_date'],infer_datetime_format=True)
   df_admission = df_admission.drop_duplicates()
  
   #drop cases with negaive lenght of service use
   df_admission = df_admission.drop(df_admission.loc[(df_admission.episode_to_date- df_admissions.episode_start_date).dt.days<0].index)
   
   return df_admissions

def aggregate_to_person_FY(df_admission,financial_year_start,financial_year_end):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_admission = df_admission.merge(df_admission.apply(lambda s:pd.date_range(str(s.episode_start_date)[:10] , s.episode_to_date,freq='AS-JUL'),1)
                                          .explode()
                                          .rename('HosAd')
                                          .drop(columns=['episode_start_date','episode_to_date'],axis=0)
                                          lef_index=True,
                                          right_index=True)
  
  df_admission.HosAd = np.where(df_admission.HosAd.isnull(),df_admission.episode_start_date.dt.to_period('Y'), df_admission.HosAd)
  df_admission.HosAd = df_admission.HosAd.apply(lambda x: str(x)[2:]+str(int(str(x))+1)[2:])
  
  df_admission= pd.get_dummies(df_admission, columns=['HosAd'])
   
  #define a new naming for new columns
  years = list(range(int(financial_year_start[2:]),int(financial_year_end[2:])+1))
  usecols = ['HosAd_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_admission = df_admission.loc[df_admission[usecols].sum(1)>0]
  
  #keep only the columns needed and remove duplicates 
  usecols = ["ppn"] + usecols
  df_admission = df_admission[usecols]
  df_admission = df_admission.groupby('ppn').max().reset_index() 
  df_admission = df_admission.drop_duplicates()
  
  return df_admission
  

if __name__ == '__main__':

  df_admission = read_hospital_admission_data()
  df_admission = aggregate_to_person_FY(df_admission,financial_year_start,financial_year_end)
  df_admission.to_csv(OutputDir + outcome_file_name + '_'+ financial_year_start + '_' +financial_year_end + '.csv' , index=False)

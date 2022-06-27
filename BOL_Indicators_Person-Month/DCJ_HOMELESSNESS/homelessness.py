import glob
import pandas as pd
import numpy as np
from datetime import datetime


#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_HOMELESSNESS/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_HOMELESSNESS/"

general_cols       = ["ppn", "ReportingPeriod_YTD", "Initial_Ass_Request", "Initial_SP_Start", "Latest_SP_Finished", "Organisation_ID_hashed"]
assist_reason_cols = ['Assist_reason14', 'Assist_reason21', 'Assist_reason24', 'Assist_reason9', 'Assist_reason13',
                    'Assist_reason15', 'Assist_reason23', 'Assist_reason17', 'Assist_reason2', 'Assist_reason3',
                    'Assist_reason4', 'Assist_reason22', 'Assist_reason25', 'Assist_reason12', 'Assist_reason11',
                    'Assist_reason10', 'Assist_reason26', 'Assist_reason5', 'Assist_reason7', 'Assist_reason1',
                    'Assist_reason8', 'Assist_reason6', 'Assist_reason18', 'Assist_reason19', 'Assist_reason20',
                    'Assist_reason16', 'Assist_reason99']
need_cols          = ['Need_21', 'Need_23', 'Need_6', 'Need_14', 'Need_13', 'Need_16', 'Need_52', 'Need_5', 'Need_4','Need_17', 'Need_7', 'Need_50', 
                     'Need_22', 'Need_25', 'Need_27', 'Need_33', 'Need_35', 'Need_46','Need_20', 'Need_51', 'Need_47', 'Need_10', 'Need_8', 'Need_15',
                     'Need_40', 'Need_45', 'Need_11','Need_43', 'Need_42', 'Need_49', 'Need_29', 'Need_19', 'Need_18', 'Need_3', 'Need_12', 'Need_28',
                     'Need_2', 'Need_38', 'Need_32', 'Need_53', 'Need_34', 'Need_41', 'Need_39', 'Need_44', 'Need_37', 'Need_36', 'Need_30', 'Need_24', 
                     'Need_1', 'Need_48', 'Need_26', 'Need_9', 'Need_31']
RP_cols            = ['RP_1', 'RP_2', 'RP_3', 'RP_4', 'RP_5', 'RP_6', 'RP_7', 'RP_8', 'RP_9', 'RP_10', 'RP_11', 'RP_12', 'RP_13','RP_14', 'RP_15',
                      'RP_16', 'RP_17', 'RP_18', 'RP_19', 'RP_20', 'RP_21', 'RP_22', 'RP_23', 'RP_24', 'RP_25', 'RP_26', 'RP_27', 'RP_28', 'RP_29',
                      'RP_30', 'RP_31', 'RP_32', 'RP_33', 'RP_34', 'RP_35', 'RP_36', 'RP_37', 'RP_38', 'RP_39', 'RP_40', 'RP_41', 'RP_42', 'RP_43', 
                      'RP_44', 'RP_45', 'RP_46', 'RP_47', 'RP_48', 'RP_49', 'RP_50', 'RP_51', 'RP_52', 'RP_53']





def read_homelessness_data(file):
  
    df = pd.read_csv(file, usecols = [*general_cols , *RP_cols, *need_cols, *assist_reason_cols], parse_dates = ['ReportingPeriod_YTD'], infer_datetime_format=True)
    
    #indicate column datatype for binary values for less memory use
    for col in df.columns if col not in general_cols:
      df[col] = df[col].fillna(0).astype(np.int8)
    
    
    df = df.drop_duplicates()

    return df
  

 def assist_reason_agg(df):
  
    # 27 Assist Reason categories are grouped into 6 categories (see https://www.aihw.gov.au/reports/homelessness-services/specialist-homelessness-services-annual-report/data for giudline)

    #Accomodation 
    df['AssistReason_Accomodation'] = df[['Assist_reason3', 'Assist_reason4', 'Assist_reason5']].max(axis=1).astype(np.int8)

    #Finantional
    df['AssistReason_Finantional'] = df[['Assist_reason1', 'Assist_reason2', 'Assist_reason15', 'Assist_reason16', 'Assist_reason17']].max(axis=1).astype(np.int8)

    #Health
    df['AssistReason_Health'] = df[['Assist_reason11', 'Assist_reason12', 'Assist_reason13', 'Assist_reason14']].max(axis=1).astype(np.int8)

    #Interpersonal Relationships
    df['AssistReason_InterpersonalRelationships'] = df[['Assist_reason6', 'Assist_reason7', 'Assist_reason8', 'Assist_reason9', 'Assist_reason10']].max(axis=1).astype(np.int8)

    #Others
    df['AssistReason_Others'] = df[['Assist_reason24', 'Assist_reason23', 'Assist_reason18', 'Assist_reason19', 'Assist_reason20','Assist_reason21', 'Assist_reason22', 'Assist_reason25', 'Assist_reason26']].max(axis=1).astype(np.int8)

    #Not Stated
    df['AssistReason_NotStated'] = df[['Assist_reason99']].max(axis=1).astype(np.int8)

    df = df.drop(assist_reason_cols, axis=1)

    return df
  
def need_agg(df):
    
    # 53 need categories are grouped into 10 categories (see https://www.aihw.gov.au/reports/homelessness-services/specialist-homelessness-services-annual-report/data for giudline)
    #Accomodation Provision
    df['Need_AccomodationProvision'] = df[['Need_1', 'Need_2', 'Need_3']].max(axis=1).astype(np.int8)
    
    #Assistance to sustain housing tenure
    df['Need_AssistanceToSustainHousingTenure'] = df[['Need_4', 'Need_5']].max(axis=1).astype(np.int8)
  
    #Dissability Services
    df['Need_Dissability'] = df[['Need_41', 'Need_42']].max(axis=1).astype(np.int8)
    
    #Drug and Alcohol Services
    df['Need_DrugAlcohol'] = df[['Need_47']].max(axis=1).astype(np.int8)
    
    #Family Services   
    df['Need_Family'] = df[['Need_33', 'Need_35', 'Need_40', 'Need_34', 'Need_39']].max(axis=1).astype(np.int8)
    
    #General Services   
    df['Need_General'] = df[
        ['Need_21', 'Need_23', 'Need_6', 'Need_14', 'Need_13', 'Need_16', 'Need_17', 'Need_7', 'Need_22', 'Need_25',
         'Need_27', 'Need_20', 'Need_10', 'Need_8', 'Need_15', 'Need_29', 'Need_19', 'Need_18', 'Need_12', 'Need_28',
         'Need_32', 'Need_30', 'Need_24', 'Need_26', 'Need_9', 'Need_31']].max(axis=1).astype(np.int8)

    #Imigration and Cultural Services   
    df['Need_ImigrationCultural'] = df[['Need_52', 'Need_50', 'Need_51', 'Need_49']].max(axis=1).astype(np.int8)
    
    #Legal Finantial Services   
    df['Need_LegalFinantial'] = df[['Need_46', 'Need_45', 'Need_11', 'Need_44']].max(axis=1).astype(np.int8)
    
    #Mental Health Services    
    df['Need_MentalHealth'] = df[['Need_38', 'Need_37', 'Need_36']].max(axis=1).astype(np.int8)

    #Other Specialist Services    
    df['Need_Other'] = df[['Need_43', 'Need_53', 'Need_48']].max(axis=1).astype(np.int8)

    df = df.drop(need_cols, axis=1)
    
    return df
  
  

def met_needs_agg(col):
  
    #If any Response is Provided to a person: needs are met if values in a column are 1 = 'Provided and referred', 2 = 'Provided but not referred', 3 = 'Referred only but not provided' 
    return df[col].apply(lambda x: np.where((x == 1)| (x == 2) | (x == 3)  , 1, 0),meta=(col,np.int8))

def RP_agg(df):
    # 53 Response categories are grouped into 10 categories (see https://www.aihw.gov.au/reports/homelessness-services/specialist-homelessness-services-annual-report/data for giudline)

    #Accomodation Provision
    df['RP_AccomodationProvision'] = pd.concat([met_needs_agg('RP_1') , met_needs_agg('RP_2') , met_needs_agg('RP_3')], axis=1).max(axis=1).astype(np.int8)

    #Assistance to sustain housing tenure
    df['RP_AssistanceToSustainHousingTenure'] = pd.concat([met_needs_agg('RP_4') , met_needs_agg('RP_5')], axis=1).max(axis=1).astype(np.int8)
    
    #Dissability Services
    df['RP_Dissability'] = pd.concat([met_needs_agg('RP_41') , met_needs_agg('RP_42')], axis=1).max(axis=1).astype(np.int8)

    #Drug and Alcohol Services
    df['RP_DrugAlcohol'] = met_needs_agg('RP_47').astype(np.int8)
    
    #Family Services   
    df['RP_Family'] = pd.concat([met_needs_agg('RP_33') , met_needs_agg('RP_35') , met_needs_agg('RP_40') , met_needs_agg('RP_34') , met_needs_agg('RP_39')], axis=1).max(axis=1).astype(
        np.int8)

    #General Services   
    df['RP_General'] = pd.concat([
                met_needs_agg('RP_21') , met_needs_agg('RP_23') , met_needs_agg('RP_6') , met_needs_agg('RP_14') , met_needs_agg('RP_13') , met_needs_agg(
            'RP_16') , met_needs_agg('RP_17') , met_needs_agg('RP_7') , met_needs_agg('RP_22') , met_needs_agg('RP_25') , met_needs_agg('RP_27') , met_needs_agg(
            'RP_20') , met_needs_agg('RP_10') , met_needs_agg('RP_8') , met_needs_agg('RP_15') , met_needs_agg('RP_29') , met_needs_agg('RP_19') , met_needs_agg(
            'RP_18') , met_needs_agg('RP_12') , met_needs_agg('RP_28') , met_needs_agg('RP_32') , met_needs_agg('RP_30') , met_needs_agg('RP_24') , met_needs_agg(
            'RP_26') , met_needs_agg('RP_9') , met_needs_agg('RP_31')], axis=1).max(axis=1).astype(np.int8)
    
    #Imigration and Cultural Services   
    df['RP_ImigrationCultural'] = pd.concat([
                met_needs_agg('RP_52') , met_needs_agg('RP_50') , met_needs_agg('RP_51') , met_needs_agg('RP_49')], axis=1).max(axis=1).astype(np.int8)
    
    #Legal Finantial Services   
    df['Need_LegalFinantial'] = pd.concat([
                met_needs_agg('RP_46') , met_needs_agg('RP_45') , met_needs_agg('RP_11') , met_needs_agg('RP_44')], axis=1).max(axis=1).astype(np.int8)
    
    #Mental Health Services    
    df['RP_MentalHealth'] = pd.concat([met_needs_agg('RP_38') , met_needs_agg('RP_37') , met_needs_agg('RP_36')], axis=1).max(axis=1).astype(np.int8)
    
    #Other Specialist Services    
    df['RP_Other'] = pd.concat([met_needs_agg('RP_43') , met_needs_agg('RP_53') , met_needs_agg('RP_48')], axis=1).max(axis=1).astype(np.int8)
    
    df = df.drop(RP_cols, axis=1)
    
    return df
  

  
  
  

if __name__ == '__main__':
  
  #get file paths for all homelessness input files
  inputFiles = glob.glob(inputDir + "/*.csv")

  for f in inputFiles:

      # read file
      print('reading: ',f)
      df = read_homelessness_data(f)
      
      # aggregate Assist_reason columns following AIHW guidelines
      print('aggregating Assist_reason columns following AIHW guidelines...')
      df = assist_reason_agg(df)

      # aggregate need columns following AIHW guidelines
      print('aggregating Need columns following AIHW guidelines...')
      df = need_agg(df)

      # aggregate Response_Provided columns following AIHW guidelines
      print("aggregating Response_Provided columns following AIHW guidelines...")
      df = RP_agg(df)


      #Keep the month resolution from ReportingPeriod_YTD column and keep the first day of the month
      df["ReportingPeriod_YTD"]=df["ReportingPeriod_YTD"].dt.strftime("%Y-%m")
      df["ReportingPeriod_YTD"]=df["ReportingPeriod_YTD"].apply(lambda x: x+'-01')
      
      df = df.rename(columns = {"ReportingPeriod_YTD": 'month'})


      #export csv file
      print('exporting results as csv file in:',outputDir+f.split('\\')[1])
      df.to_csv(outputDir+f.split('\\')[1] , index=False)
        
      # free memory
      del df


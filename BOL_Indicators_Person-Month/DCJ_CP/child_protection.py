import pandas as pd
import numpy as np


#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_CP/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_CP/"

file = 'PRJ_2021_40_3_Child_Protection_File_30JUNE_SENSITIVE.csv'



def read_concern_reports_data(inputDir,file):
    print('>>>Reading data for concern reports ... ' )

    concern_reports_columns=['PPN',
                             'ContactStartDateDate', 
                             'ContactorTypeGroup'
                              ]
    #read csv source with columns related to concern reports
    df_concern_reports = pd.read_csv(inputDir + file, usecols=concern_reports_columns,
                                     parse_dates=['ContactStartDateDate'], infer_datetime_format=True)
                                     
    #define a new column to capture ConcernReports                                 
    df_concern_reports['ConcernReports'] = 1

    #add new columns to encode ContactorTypeGroup                                 
    df_concern_reports[['ReportedByNSWPolice','ReportedByNSWHealth','ReportedByFACS','ReportedByNSWEducation','ReportedByOther','ReportedByNGO']] = 0

    #encode ContactorTypeGroup and drop the column next
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Police', 'ReportedByNSWPolice'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Health', 'ReportedByNSWHealth'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'FACS', 'ReportedByFACS'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Education', 'ReportedByNSWEducation'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NGO', 'ReportedByNGO'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup.isin(['Non-mandatory reporters','Other mandatory','Catholic/Independent schools','Child care/Preschool','Other Health']), 'ReportedByOther'] = 1

    df_concern_reports=df_concern_reports.drop(columns=['ContactorTypeGroup'],axis=1)
    
    #for flags variables, change column types to np.int8 for lower memory use 
    df_concern_reports[df_concern_reports.columns[2:]] = df_concern_reports[df_concern_reports.columns[2:]].astype(np.int8)
    
    
    return df_concern_reports

def aggregate_person_month_concern_reports(df):
    print('>>>Aggregating person-month concern reports ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="ContactStartDateDate", freq="M")]).agg({
                                                                                "ConcernReports": "max",
                                                                                'ReportedByNSWPolice':'max',
                                                                                'ReportedByNSWHealth': 'max',
                                                                                'ReportedByFACS':'max',
                                                                                'ReportedByNSWEducation': 'max',
                                                                                'ReportedByNGO': 'max',
                                                                                'ReportedByOther':'max'}).reset_index()

    df['ContactStartDateDate']=df['ContactStartDateDate'].dt.strftime("%Y-%m")+'-01'
    df=df.rename(columns={'ContactStartDateDate': 'month'})
    
    return df

def read_ROSH_reports_data(inputDir,file):
    print('>>>Reading the data for ROSH reports ... ' )

    ROSH_reports_columns=['PPN','ContactStartDateDate',
                         'HelplineAssessmentStartDate', 
                         'HA_CarerIssuesFlag',  
                         'HA_CYPIssuesFlag',  
                         'HA_DomesticViolenceFlag', 
                         'HA_EmotionalAbuseFlag',  
                         'HA_NeglectFlag',  
                         'HA_PhysicalAbuseFlag',  
                         'HA_SexualAbuseFlag',  
                         'HA_OtherIssuesFlag',  
                         'ROSHFlag']
                         
    #read csv source with columns related to ROSH reports
    df_ROSH = pd.read_csv(inputDir+file,usecols=ROSH_reports_columns, parse_dates=['ContactStartDateDate','HelplineAssessmentStartDate'], infer_datetime_format=True)
        
    #encode HA_
    df_ROSH['HA_CarerIssuesFlag'].replace({'No Carer Issues': 0,'Missing/NA': 0, 'Carer Issues': 1}, inplace=True)
    df_ROSH['HA_CYPIssuesFlag'].replace({'No CYP Issues': 0,'Missing/NA': 0,  'CYP Issues': 1}, inplace=True)
    df_ROSH['HA_DomesticViolenceFlag'].replace({'No Domestic Violence': 0,'Missing/NA': 0,  'Domestic Violence': 1}, inplace=True)
    df_ROSH['HA_EmotionalAbuseFlag'].replace({'No Emotional Abuse': 0,'Missing/NA': 0,  'Emotional Abuse': 1}, inplace=True)
    df_ROSH['HA_NeglectFlag'].replace({'No Neglect': 0, 'Missing/NA': 0, 'Neglect': 1}, inplace=True)
    df_ROSH['HA_PhysicalAbuseFlag'].replace({'No Physical Abuse': 0,'Missing/NA': 0,  'Physical Abuse': 1}, inplace=True)
    df_ROSH['HA_SexualAbuseFlag'].replace({'No Sexual Abuse': 0,'Missing/NA': 0,  'Sexual Abuse': 1}, inplace=True)
    df_ROSH['HA_OtherIssuesFlag'].replace({'No Other Issues': 0,'Missing/NA': 0,  'Other Issues': 1}, inplace=True)
    df_ROSH['ROSHFlag'].replace({'CYP is not at ROSH': 0, 'CYP is at ROSH': 1}, inplace=True)
    


    #take the maximum recorded date to capture the month value
    df_ROSH['max_recorded_date'] = df_ROSH[['ContactStartDateDate','HelplineAssessmentStartDate']].max(axis=1) 
    df_ROSH = df_ROSH.drop(columns=['ContactStartDateDate','HelplineAssessmentStartDate'],axis=1)
 
    #for flags variables, fill missing values and change column types to np.int8 for lower memory use 
    df_ROSH[df_ROSH.columns[1:-1]] = df_ROSH[df_ROSH.columns[1:-1]].fillna(0).astype(np.int8)
 

    
    return df_ROSH  
  
def aggregate_person_month_ROSH(df):
    print('>>>Aggregating person-month ROSH ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="max_recorded_date", freq="M")]).agg({
        "ROSHFlag": "max",
        'HA_CarerIssuesFlag': 'max',
        'HA_CYPIssuesFlag': 'max',
        'HA_DomesticViolenceFlag': 'max',
        'HA_EmotionalAbuseFlag': 'max',
        'HA_SexualAbuseFlag': 'max',
        'HA_PhysicalAbuseFlag': 'max'}).reset_index()

    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'
    
    df = df.rename(columns={'max_recorded_date': 'month',
                            'ROSHFlag' : 'ROSH',
                            'HA_CarerIssuesFlag': 'HA_CarerIssues',
                            'HA_CYPIssuesFlag' : 'HA_CYPIssues',
                            'HA_DomesticViolenceFlag': 'HA_DomesticViolence',
                            'HA_EmotionalAbuseFlag': 'HA_EmotionalAbuse',
                            'HA_SexualAbuseFlag': 'HA_SexualAbuse',
                            'HA_PhysicalAbuseFlag': 'HA_PhysicalAbuse'
                            'HA_PhysicalAbuseFlag': 'HA_PhysicalAbuse'})
                           
    return df

def read_SARA_reports_data(inputDir, file):
    print('>>>Reading the data for SARA reports ... ' )

    SARA_reports_columns = ['PPN',
                            'ContactStartDateDate',
                            'FieldAssessmentStartDateDate',
                            'FieldAssessmentPathway',
                            'FA_CarerIssuesFlag',
                            'FA_CYPIssuesFlag',
                            'FA_DomesticViolenceFlag',
                            'FA_EmotionalAbuseFlag',
                            'FA_NeglectFlag',
                            'FA_OtherIssuesFlag',
                            'FA_PhysicalAbuseFlag',
                            'FA_SexualAbuseFlag',
                            '_4nTriageDecision',
                            '_5nOutcomeOfFieldAssessment']
                            
    #read csv source with columns related to SARA reports
    df_SARA = pd.read_csv(inputDir + file, usecols=SARA_reports_columns, parse_dates=['ContactStartDateDate','FieldAssessmentStartDateDate'],
                          infer_datetime_format=True)
                          
    #encode FA_, SARA, _4nTriageDecision, and _5nOutcomeOfFieldAssessment
    df_SARA['FA_CarerIssuesFlag'].replace({'Missing/NA':0,'No Carer Issues': 0, 'Carer Issues': 1}, inplace=True)
    df_SARA['FA_CYPIssuesFlag'].replace({'Missing/NA':0,'No CYP Issues': 0, 'CYP Issues': 1}, inplace=True)
    df_SARA['FA_DomesticViolenceFlag'].replace({'Missing/NA':0,'No Domestic Violence': 0, 'Domestic Violence': 1}, inplace=True)
    df_SARA['FA_EmotionalAbuseFlag'].replace({'Missing/NA':0,'No Emotional Abuse': 0, 'Emotional Abuse': 1}, inplace=True)
    df_SARA['FA_NeglectFlag'].replace({'Missing/NA':0,'No Neglect': 0, 'Neglect': 1}, inplace=True)
    df_SARA['FA_OtherIssuesFlag'].replace({'Missing/NA':0,'No Other Issues': 0, 'Other Issues': 1}, inplace=True)
    df_SARA['FA_PhysicalAbuseFlag'].replace({'Missing/NA':0,'No Physical Abuse': 0, 'Physical Abuse': 1}, inplace=True)
    df_SARA['FA_SexualAbuseFlag'].replace({'Missing/NA':0,'No Sexual Abuse': 0, 'Sexual Abuse': 1}, inplace=True)
    df_SARA['FieldAssessmentPathway'].replace({'SARA': 1, 'Missing/NA': 0, 'Alternative Assessment': 0}, inplace=True)
     
    df_SARA['TriageDecision_Allocate'] = 0
    df_SARA.loc[df_SARA._4nTriageDecision == '4A - Triage decision to allocate', 'TriageDecision_Allocate'] = 1
    
    df_SARA['FAOutcome_Substantiated'] = 0
    df_SARA.loc[
        df_SARA._5nOutcomeOfFieldAssessment == '5A1 - Field Assessment Complete: Substantiated', 'FAOutcome_Substantiated'] = 1

    #take the maximum recorded date to capture the month value
    df_SARA['max_recorded_date'] = df_SARA[['ContactStartDateDate','FieldAssessmentStartDateDate']].max(axis=1)#there are 2.88% with 'HelplineAssessmentStartDate'<'ContactStartDateDate'
    df_SARA = df_SARA.drop(['_4nTriageDecision', '_5nOutcomeOfFieldAssessment','ContactStartDateDate','FieldAssessmentStartDateDate'], axis=1)

    
    #for flags variables, fill missing values and change column types to np.int8 for lower memory use 
    df_SARA[df_SARA.columns[1:-1]] = df_SARA[df_SARA.columns[1:-1]].fillna(0).astype(np.int8)
    
    
    return df_SARA
  
def aggregate_person_month_SARA(df):
    print('>>>Aggregating person-month SARA ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="max_recorded_date", freq="M")]).agg({
        "FieldAssessmentPathway": "max",
        'FAOutcome_Substantiated': 'max',
        'TriageDecision_Allocate': 'max',
        'FA_DomesticViolenceFlag': 'max',
        'FA_EmotionalAbuseFlag': 'max',
        'FA_NeglectFlag': 'max',
        'FA_OtherIssuesFlag': 'max',
        'FA_PhysicalAbuseFlag': 'max',
        'FA_SexualAbuseFlag': 'max',
        'FA_CarerIssuesFlag': 'max',
        'FA_CYPIssuesFlag': 'max'})

    df = df.reset_index()
    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'

    df = df.rename(columns={'max_recorded_date': 'month',
                            'FieldAssessmentPathway':'SARA',
                            'FA_DomesticViolenceFlag': 'FA_DomesticViolence',
                            'FA_EmotionalAbuseFlag':'FA_EmotionalAbuse',
                            'FA_NeglectFlag':'FA_Neglect',
                            'FA_OtherIssuesFlag':'FA_OtherIssues',
                            'FA_PhysicalAbuseFlag':'FA_PhysicalAbuse',
                            'FA_SexualAbuseFlag':'FA_SexualAbuse'
                            'FA_CarerIssuesFlag':'FA_CarerIssues',
                            'FA_CYPIssuesFlag':'FA_CYPIssues'
                           })
    return df
  
def read_Safety_reports_data(inputDir, file):
    print('>>>Reading the data for Safety reports ... ' )

    Safety_reports_columns = ['PPN',
                            'ContactStartDateDate',
                            'SafetyAssessmentStartDate',
                            'SafetyOutcome']
                             
    #read csv source with columns related to Safety reports
    df_Safety = pd.read_csv(inputDir + file, usecols=Safety_reports_columns, parse_dates=['ContactStartDateDate','SafetyAssessmentStartDate'],
                            infer_datetime_format=True)

    #encode SafetyOutcome
    df_Safety[['SafetyOutcome_SafeFlag','SafetyOutcome_SafeWithPlanFlag','SafetyOutcome_Unsafe']] = 0

    df_Safety.loc[df_Safety.SafetyOutcome == 'Safe', 'SafetyOutcome_SafeFlag'] = 1
    df_Safety.loc[df_Safety.SafetyOutcome == 'Safe with plan', 'SafetyOutcome_SafeWithPlanFlag'] = 1
    df_Safety.loc[df_Safety.SafetyOutcome == 'Unsafe', 'SafetyOutcome_Unsafe'] = 1
    
    df_Safety = df_Safety.drop(['SafetyOutcome'], axis=1)
                            
    #take the maximum recorded date to capture the month value
    df_Safety['max_recorded_date'] = df_Safety[['ContactStartDateDate','SafetyAssessmentStartDate']].max(axis=1) 
    df_Safety = df_Safety.drop(['ContactStartDateDate','SafetyAssessmentStartDate'], axis=1)


    return df_Safety

def aggregate_person_month_Safety(df):
    print('>>>Aggregating person-month Safety ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="max_recorded_date", freq="M")]).agg({
        'SafetyOutcome_SafeFlag':"max",
        'SafetyOutcome_SafeWithPlanFlag':"max",
        'SafetyOutcome_Unsafe':"max"})

    df = df.reset_index()
    df['SaftyOutcome'] = 'Missing/NA'
    df.loc[df.SafetyOutcome_SafeFlag == 1 , 'SaftyOutcome'] = 'Safe'
    df.loc[df.SafetyOutcome_SafeWithPlanFlag == 1 , 'SaftyOutcome'] = 'SafeWithPlan'
    df.loc[df.SafetyOutcome_Unsafe == 1 , 'SaftyOutcome'] = 'Unsafe'

    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'
    df = df.drop(['SafetyOutcome_SafeFlag','SafetyOutcome_SafeWithPlanFlag','SafetyOutcome_Unsafe'] , axis=1)
    
    df = df.rename(columns={'max_recorded_date': 'month'})
    
    return df
  
def read_Final_RISK_reports_data(inputDir, file):
    print('>>>Reading the data for RISK reports ... ' )

    Risk_reports_columns=['PPN', 'ContactStartDateDate','RiskAssessmentStartDate','FinalRiskLevel']
    df_RA = pd.read_csv(inputDir+file,usecols=Risk_reports_columns, parse_dates=['ContactStartDateDate','RiskAssessmentStartDate'], infer_datetime_format=True)

    #take the maximum recorded date to capture the month value
    df_RA['max_recorded_date'] = df_RA[['ContactStartDateDate','RiskAssessmentStartDate']].max(axis=1)
    
    #encode FinalRiskLevel
    df_RA[['FinalRiskLevel_High','FinalRiskLevel_Medium/Moderate','FinalRiskLevel_VeryHigh','FinalRiskLevel_Low','FinalRiskLevel_Missing/NA'] = 0

    df_RA.loc[df_RA.FinalRiskLevel == 'High', 'FinalRiskLevel_High'] = 1
    df_RA.loc[df_RA.FinalRiskLevel == 'Medium/Moderate', 'FinalRiskLevel_Medium/Moderate'] = 1
    df_RA.loc[df_RA.FinalRiskLevel == 'very High', 'FinalRiskLevel_VeryHigh'] = 1
    df_RA.loc[df_RA.FinalRiskLevel == 'Low', 'FinalRiskLevel_Low'] = 1
    df_RA.loc[df_RA.FinalRiskLevel == 'Missing/NA', 'FinalRiskLevel_Missing/NA'] = 1
    
    df_RA = df_RA.drop(['FinalRiskLevel'], axis=1)

    return df_RA

def aggregate_person_month_Final_RISK(df):
    print('>>>Aggregating person-month RISK ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="max_recorded_date", freq="M")]).agg({
        'FinalRiskLevel_High': 'max',
        'FinalRiskLevel_Medium/Moderate': 'max',
        'FinalRiskLevel_VeryHigh': 'max',
        'FinalRiskLevel_Low': 'max',
        'FinalRiskLevel_Missing/NA': 'max'}).reset_index()

    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'
    df = df.rename(columns={'max_recorded_date': 'month'})
    
    return df

def read_Alternate_RISK_reports_data(inputDir, file):
    print('>>>Reading the data for Alternate RISK reports ... ' )

    Alternate_Risk_reports_columns=['PPN', 'ContactStartDateDate','AlternateAssessmentStartDate','AlternateAssessmentRiskLevel']
    df_RA = pd.read_csv(inputDir+file,usecols=Alternate_Risk_reports_columns, parse_dates=['ContactStartDateDate','RiskAssessmentStartDate'], infer_datetime_format=True)

    
    #take the maximum recorded date to capture the month value
    df_RA['max_recorded_date'] = df_RA[['ContactStartDateDate','RiskAssessmentStartDate']].max(axis=1)
    
    
    #encode FinalRiskLevel
    df_RA[['AltRiskLevel_High','AltRiskLevel_Medium/Moderate','AltRiskLevel_VeryHigh','AltRiskLevel_Low','AltRiskLevel_Missing/NA'] = 0
    df_RA.loc[df_RA.AlternateAssessmentRiskLevel == 'High', 'AltRiskLevel_High'] = 1
    df_RA.loc[df_RA.AlternateAssessmentRiskLevel == 'Medium/Moderate', 'AltRiskLevel_Medium/Moderate'] = 1
    df_RA.loc[df_RA.AlternateAssessmentRiskLevel == 'very High', 'AltRiskLevel_VeryHigh'] = 1
    df_RA.loc[df_RA.AlternateAssessmentRiskLevel == 'Low', 'AltRiskLevel_Low'] = 1
    df_RA.loc[df_RA.AlternateAssessmentRiskLevel == 'Missing/NA', 'AltRiskLevel_Missing/NA'] = 1
    df_RA = df_RA.drop(['AlternateAssessmentRiskLevel'], axis=1)

    return df_RA  

  def aggregate_person_month_Alternate_RISK(df):
    print('>>>Aggregating person-month Alternate RISK ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="max_recorded_date", freq="M")]).agg({
        'AltRiskLevel_High': 'max',
        'AltRiskLevel_Medium/Moderate': 'max',
        'AltRiskLevel_VeryHigh': 'max',
        'AltRiskLevel_Low': 'max',
        'AltRiskLevel_Missing/NA': 'max'}).reset_index()
        
    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'
    df = df.rename(columns={'max_recorded_date': 'month'})
    
    return df  
  
def summorize_person_month_Risk(df_final, df_alt):
    print('>>>Summorizing person_month Risk ... ' )
    df = merge_ful_join(df_final, df_alt)
    df['RiskLevel'] = 'Missing/NA'
    df.loc[df['AltRiskLevel_Low']==1 , 'RiskLevel']='Low'
    df.loc[df['FinalRiskLevel_Low']==1 , 'RiskLevel']='Low'
    df.loc[df['AltRiskLevel_Medium/Moderate']==1 , 'RiskLevel']='Medium/Moderate'
    df.loc[df['FinalRiskLevel_Medium/Moderate']==1 , 'RiskLevel']='Medium/Moderate'
    df.loc[df['AltRiskLevel_High']==1 , 'RiskLevel']='High'
    df.loc[df['FinalRiskLevel_High']==1 , 'RiskLevel']='High'
    df.loc[df['AltRiskLevel_VeryHigh']==1 , 'RiskLevel']='VeryHigh'
    df.loc[df['FinalRiskLevel_VeryHigh']==1 , 'RiskLevel']='VeryHigh'
    df = df.drop(['AltRiskLevel_Low','FinalRiskLevel_Low','AltRiskLevel_Medium/Moderate',
                 'FinalRiskLevel_Medium/Moderate','AltRiskLevel_High','FinalRiskLevel_High',
                 'AltRiskLevel_VeryHigh', 'FinalRiskLevel_VeryHigh'] , axis=1)
    df = df.drop_duplicates()
    df = df[df.RiskLevel != 0]
    
    return df
  

def merge_full_join(df1,df2):
    df_merge=pd.merge(df1, df2, on=['PPN', 'month'], how='outer')
    df_merge=df_merge.fillna(0)
    for col in [i for i in df_merge.columns if i not in ['PPN', 'month'] ]:
        df_merge[col]=df_merge[col].astype(np.int8)
    return df_merge
  

    
if __name__ == '__main__':


    df_concern_reports = read_concern_reports_data(inputDir, file)
    df_concern_reports = aggregate_person_month_concern_reports(df_concern_reports)

    df_ROSH=read_ROSH_reports_data(inputDir, file)
    df_ROSH = aggregate_person_month_ROSH(df_ROSH)
    df_merged=merge_full_join(df_concern_reports,df_ROSH)
    del df_ROSH,df_concern_reports

    df_SARA=read_SARA_reports_data(inputDir, file)
    df_SARA = aggregate_person_month_SARA(df_SARA)
    df_merged=merge_full_join(df_merged,df_SARA)
    del df_SARA

    df_Safety=read_Safety_reports_data(inputDir, file)
    df_Safety = aggregate_person_month_Safety(df_Safety)
    df_merged=merge_full_join(df_merged,df_Safety)
    del df_Safety

    df_Final_RISK=read_Final_RISK_reports_data(inputDir, file)
    df_Final_RISK = aggregate_person_month_Final_RISK(df_Final_RISK)
    df_Alt_RISK=read_Final_Alternate_reports_data(inputDir, file)
    df_Alt_RISK = aggregate_person_month_Alternate_RISK(df_Alt_RISK)
    df_RISK =  summorize_person_month_Risk(df_Final_RISK , df_Alt_RISK)
    df_merged=merge_full_join(df_merged,df_RISK)
    del df_RISK


    #save the final csv file
    df_merged.to_csv(outputDir+file , index= False)






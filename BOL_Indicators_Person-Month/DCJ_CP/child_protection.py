import pandas as pd
import numpy as np


#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_CP/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_CP/"

def read_concern_reports_data(inputDir,file):
    print('>>>Reading data for concern reports ... ' )

    concern_reports_columns=['PPN',
                             'ContactStartDateDate', 
                             'ContactorTypeGroup', #'Non-mandatory reporters', 'NSW Education','NGO','NSW Police','NSW Health', 'FACS','Other mandatory','Catholic/Independent schools','Child care/Preschool','Other Health'
                             'PseudoReportFlag'    #'Not a pseudo report','Pseudo report'
                              ]
    df_concern_reports = pd.read_csv(inputDir + file, usecols=concern_reports_columns,
                                     parse_dates=['ContactStartDateDate'], infer_datetime_format=True)
    df_concern_reports['concern_reports'] = 1
    df_concern_reports['PseudoReportFlag'] = df_concern_reports['PseudoReportFlag'].fillna(0).replace({'Not a pseudo report': 0, 'Pseudo report': 1}, inplace=True)

    df_concern_reports['ReportedByNSWPolice'] = 0
    df_concern_reports['ReportedByNSWHealth'] = 0
    df_concern_reports['ReportedByFACS'] = 0
    df_concern_reports['ReportedByNSWEducation'] = 0
    df_concern_reports['ReportedByOther'] = 0
    df_concern_reports['ReportedByNGO'] = 0
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Police', 'ReportedByNSWPolice'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Health', 'ReportedByNSWHealth'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'FACS', 'ReportedByFACS'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NSW Education', 'ReportedByNSWEducation'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'NGO', 'ReportedByNGO'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'Non-mandatory reporters', 'ReportedByOther'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'Other mandatory', 'ReportedByOther'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'Catholic/Independent schools', 'ReportedByOther'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'Child care/Preschool', 'ReportedByOther'] = 1
    df_concern_reports.loc[df_concern_reports.ContactorTypeGroup == 'Other Health', 'ReportedByOther'] = 1

    df_concern_reports=df_concern_reports.drop(columns=['ContactorTypeGroup'],axis=1)
    df_concern_reports=df_concern_reports.astype({'ReportedByNSWPolice':np.int8,'ReportedByNSWHealth':np.int8,
                                                  'ReportedByFACS':np.int8,'ReportedByNSWEducation':np.int8,
                                                  'ReportedByNGO':np.int8,'ReportedByOther':np.int8,
                                                  'PseudoReportFlag':np.int8,'concern_reports':np.int8})
    
    
    return df_concern_reports

def aggregate_person_month_concern_reports(df):
    print('>>>Aggregating person-month concern reports ... ' )

    df = df.groupby(['PPN', pd.Grouper(key="ContactStartDateDate", freq="M")]).agg({
                                                                                "concern_reports": "count",
                                                                                "PseudoReportFlag": "sum",
                                                                                'ReportedByNSWPolice':'max',
                                                                                'ReportedByNSWHealth': 'max',
                                                                                'ReportedByFACS':'max',
                                                                                'ReportedByNSWEducation': 'max',
                                                                                'ReportedByOther':'max',
                                                                                'ReportedByNGO': 'max'})

    df=df.reset_index()
    df['ContactStartDateDate']=df['ContactStartDateDate'].dt.strftime("%Y-%m")+'-01'
    df=df.rename(columns={'ContactStartDateDate': 'month', "PseudoReportFlag":'PseudoReports'})
    
    return df

def read_ROSH_reports_data(inputDir,file):
    print('>>>Reading the data for ROSH reports ... ' )

    ROSH_reports_columns=['PPN','ContactStartDateDate',
                 'HelplineAssessmentStartDate',
                'HA_CarerIssuesFlag',  # ('No Carer Issues', -) ('Carer Issues', -)
                 'HA_CYPIssuesFlag',  # ('No CYP Issues', -) ('CYP Issues', -)
                 'HA_DomesticViolenceFlag',  # ('No Domestic Violence', -) ('Domestic Violence', -)
                 'HA_EmotionalAbuseFlag',  # ('No Emotional Abuse', -) ('Emotional Abuse', -)
                 'HA_NeglectFlag',  # ('No Neglect', -) ('Neglect', -)
                 'HA_PhysicalAbuseFlag',  # ('No Physical Abuse', -) ('Physical Abuse', -)
                 'HA_SexualAbuseFlag',  # ('No Sexual Abuse', -) ('Sexual Abuse', -)
                 'HA_OtherIssuesFlag',  # ('No Other Issues', -) ('Other Issues', -)
                 'ROSHFlag']
    df_ROSH = pd.read_csv(inputDir+file,usecols=ROSH_reports_columns, parse_dates=['ContactStartDateDate','HelplineAssessmentStartDate'], infer_datetime_format=True)
    df_ROSH.HelplineAssessmentStartDate=pd.to_datetime(df_ROSH.HelplineAssessmentStartDate, format='%m/%d/%Y')
    ##HA_
    df_ROSH['HA_CarerIssuesFlag'].replace({'No Carer Issues': 0,'Missing/NA': 0, 'Carer Issues': 1}, inplace=True)
    df_ROSH['HA_CYPIssuesFlag'].replace({'No CYP Issues': 0,'Missing/NA': 0,  'CYP Issues': 1}, inplace=True)
    df_ROSH['HA_DomesticViolenceFlag'].replace({'No Domestic Violence': 0,'Missing/NA': 0,  'Domestic Violence': 1}, inplace=True)
    df_ROSH['HA_EmotionalAbuseFlag'].replace({'No Emotional Abuse': 0,'Missing/NA': 0,  'Emotional Abuse': 1}, inplace=True)
    df_ROSH['HA_NeglectFlag'].replace({'No Neglect': 0, 'Missing/NA': 0, 'Neglect': 1}, inplace=True)
    df_ROSH['HA_PhysicalAbuseFlag'].replace({'No Physical Abuse': 0,'Missing/NA': 0,  'Physical Abuse': 1}, inplace=True)
    df_ROSH['HA_SexualAbuseFlag'].replace({'No Sexual Abuse': 0,'Missing/NA': 0,  'Sexual Abuse': 1}, inplace=True)
    df_ROSH['HA_OtherIssuesFlag'].replace({'No Other Issues': 0,'Missing/NA': 0,  'Other Issues': 1}, inplace=True)
    df_ROSH['ROSHFlag'].replace({'CYP is not at ROSH': 0, 'CYP is at ROSH': 1}, inplace=True)
    df_ROSH['HA_CarerIssuesFlag']= df_ROSH['HA_CarerIssuesFlag'].fillna(0)
    df_ROSH['HA_CYPIssuesFlag']=  df_ROSH['HA_CYPIssuesFlag'].fillna(0)
    df_ROSH['HA_DomesticViolenceFlag']=  df_ROSH['HA_DomesticViolenceFlag'].fillna(0)
    df_ROSH['HA_EmotionalAbuseFlag']=  df_ROSH['HA_EmotionalAbuseFlag'].fillna(0)
    df_ROSH['HA_NeglectFlag']=   df_ROSH['HA_NeglectFlag'].fillna(0)
    df_ROSH['HA_PhysicalAbuseFlag']= df_ROSH['HA_PhysicalAbuseFlag'].fillna(0)
    df_ROSH['HA_SexualAbuseFlag']=  df_ROSH['HA_SexualAbuseFlag'].fillna(0)
    df_ROSH['HA_OtherIssuesFlag']=   df_ROSH['HA_OtherIssuesFlag'].fillna(0)
    df_ROSH['ROSHFlag']=  df_ROSH['ROSHFlag'].fillna(0)

    df_ROSH['max_recorded_date'] = df_ROSH[['ContactStartDateDate','HelplineAssessmentStartDate']].max(axis=1)
    df_ROSH = df_ROSH.drop(columns=['ContactStartDateDate','HelplineAssessmentStartDate'],axis=1)
    df_ROSH = df_ROSH.astype({'HA_CarerIssuesFlag':np.int8,'HA_CYPIssuesFlag':np.int8,
                                                  'HA_DomesticViolenceFlag':np.int8,'HA_PhysicalAbuseFlag':np.int8,
                                                  'HA_SexualAbuseFlag':np.int8,'HA_OtherIssuesFlag':np.int8,
                                                  'ROSHFlag':np.int8,'HA_NeglectFlag':np.int8,'HA_EmotionalAbuseFlag':np.int8})
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
        'HA_PhysicalAbuseFlag': 'max'})

    df = df.reset_index()
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
                            'FieldAssessmentPathway',# ('SARA', -) ('Missing/NA',-) ('Alternative Assessment', -)
                            'FA_CarerIssuesFlag',
                            'FA_CYPIssuesFlag',
                            'FA_DomesticViolenceFlag',
                            'FA_EmotionalAbuseFlag',
                            'FA_NeglectFlag',
                            'FA_OtherIssuesFlag',
                            'FA_PhysicalAbuseFlag',
                            'FA_SexualAbuseFlag',
                            '_4nTriageDecision',# ['4C1 - Triage decision to close: competing priorities',       '4A - Triage decision to allocate',       '4D - No Triage: Closed at Helpline',       '4C3 - Triage decision to close: other', '4B - Triage decision pending',       '4E - Triage decision Pending - No transfer record',       '4C2 - Triage decision to close: subject not located']
                            '_5nOutcomeOfFieldAssessment']
    df_SARA = pd.read_csv(inputDir + file, usecols=SARA_reports_columns, parse_dates=['ContactStartDateDate'],
                          infer_datetime_format=True)
    df_SARA.FieldAssessmentStartDateDate=pd.to_datetime(df_SARA.FieldAssessmentStartDateDate, format='%m/%d/%Y')
    df_SARA['max_recorded_date'] = df_SARA[['ContactStartDateDate','FieldAssessmentStartDateDate']].max(axis=1)


    ##FA_
    df_SARA['FA_CarerIssuesFlag'].replace({'Missing/NA':0,'No Carer Issues': 0, 'Carer Issues': 1}, inplace=True)
    df_SARA['FA_CYPIssuesFlag'].replace({'Missing/NA':0,'No CYP Issues': 0, 'CYP Issues': 1}, inplace=True)
    df_SARA['FA_DomesticViolenceFlag'].replace({'Missing/NA':0,'No Domestic Violence': 0, 'Domestic Violence': 1}, inplace=True)
    df_SARA['FA_EmotionalAbuseFlag'].replace({'Missing/NA':0,'No Emotional Abuse': 0, 'Emotional Abuse': 1}, inplace=True)
    df_SARA['FA_NeglectFlag'].replace({'Missing/NA':0,'No Neglect': 0, 'Neglect': 1}, inplace=True)
    df_SARA['FA_OtherIssuesFlag'].replace({'Missing/NA':0,'No Other Issues': 0, 'Other Issues': 1}, inplace=True)
    df_SARA['FA_PhysicalAbuseFlag'].replace({'Missing/NA':0,'No Physical Abuse': 0, 'Physical Abuse': 1}, inplace=True)
    df_SARA['FA_SexualAbuseFlag'].replace({'Missing/NA':0,'No Sexual Abuse': 0, 'Sexual Abuse': 1}, inplace=True)
    df_SARA['FA_SexualAbuseFlag'] = df_SARA['FA_SexualAbuseFlag'].fillna(0)
    df_SARA['FA_PhysicalAbuseFlag'] = df_SARA['FA_PhysicalAbuseFlag'].fillna(0)
    df_SARA['FA_OtherIssuesFlag'] = df_SARA['FA_OtherIssuesFlag'].fillna(0)
    df_SARA['FA_NeglectFlag'] = df_SARA['FA_NeglectFlag'].fillna(0)
    df_SARA['FA_EmotionalAbuseFlag'] = df_SARA['FA_EmotionalAbuseFlag'].fillna(0)
    df_SARA['FA_DomesticViolenceFlag'] = df_SARA['FA_DomesticViolenceFlag'].fillna(0)
    df_SARA['FA_DomesticViolenceFlag'] = df_SARA['FA_DomesticViolenceFlag'].fillna(0)
    df_SARA['FA_CYPIssuesFlag'] = df_SARA['FA_CYPIssuesFlag'].fillna(0)
    df_SARA['FA_CarerIssuesFlag'] = df_SARA['FA_CarerIssuesFlag'].fillna(0)
    
    df_SARA['TriageDecision_Allocate'] = 0
    df_SARA.loc[df_SARA._4nTriageDecision == '4A - Triage decision to allocate', 'TriageDecision_Allocate'] = 1
    
    df_SARA['FAOutcome_Substantiated'] = 0
    df_SARA.loc[
        df_SARA._5nOutcomeOfFieldAssessment == '5A1 - Field Assessment Complete: Substantiated', 'FAOutcome_Substantiated'] = 1

    df_SARA = df_SARA.drop(['_4nTriageDecision', '_5nOutcomeOfFieldAssessment','ContactStartDateDate','FieldAssessmentStartDateDate'], axis=1)
    
    df_SARA['FieldAssessmentPathway'].replace({'SARA': 1, 'Missing/NA': 0, 'Alternative Assessment': 0}, inplace=True)
    df_SARA['FieldAssessmentPathway'] = df_SARA['FieldAssessmentPathway'].fillna(0)
    
    df_SARA=df_SARA.astype({'FAOutcome_Substantiated':np.int8,'TriageDecision_Allocate':np.int8,
                                                  'FA_DomesticViolenceFlag':np.int8,'FA_EmotionalAbuseFlag':np.int8,'FA_NeglectFlag':np.int8,
                                                  'FA_OtherIssuesFlag':np.int8,'FA_PhysicalAbuseFlag':np.int8,'FA_SexualAbuseFlag':np.int8,
                                                  ,'FA_CarerIssuesFlag':np.int8,'FA_CYPIssuesFlag':np.int8})

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
  
def read_Safety_repots_data(inputDir, file):
    print('>>>Reading the data for Safety reports ... ' )

    Safety_repots_columns = ['PPN',
                            'ContactStartDateDate',
                            'SafetyAssessmentStartDate',
                             'SafetyOutcome']
    df_Safety = pd.read_csv(inputDir + file, usecols=Safety_repots_columns, parse_dates=['ContactStartDateDate','SafetyAssessmentStartDate'],
                            infer_datetime_format=True)
    df_Safety.SafetyAssessmentStartDate=pd.to_datetime(df_Safety.SafetyAssessmentStartDate, format='%m/%d/%Y')
    df_Safety['max_recorded_date'] = df_Safety[['ContactStartDateDate','SafetyAssessmentStartDate']].max(axis=1)
    ##SafetyOutcome
    df_Safety['SafetyOutcome_SafeFlag'] = 0
    df_Safety['SafetyOutcome_SafeWithPlanFlag'] = 0
    df_Safety['SafetyOutcome_Unsafe'] = 0
    df_Safety.loc[df_Safety.SafetyOutcome == 'Safe', 'SafetyOutcome_SafeFlag'] = 1
    df_Safety.loc[df_Safety.SafetyOutcome == 'Safe with plan', 'SafetyOutcome_SafeWithPlanFlag'] = 1
    df_Safety.loc[df_Safety.SafetyOutcome == 'Unsafe', 'SafetyOutcome_Unsafe'] = 1
    df_Safety = df_Safety.drop(['SafetyOutcome'], axis=1)

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
  
def read_Final_RISK_repots_data(inputDir, file):
    print('>>>Reading the data for RISK repots ... ' )

    Risk_reports_columns=['PPN', 'ContactStartDateDate','RiskAssessmentStartDate','FinalRiskLevel']
    df_RA = pd.read_csv(inputDir+file,usecols=Risk_reports_columns, parse_dates=['ContactStartDateDate'], infer_datetime_format=True)

    df_RA.RiskAssessmentStartDate=pd.to_datetime(df_RA.RiskAssessmentStartDate, format='%m/%d/%Y')
    df_RA['max_recorded_date'] = df_RA[['ContactStartDateDate','RiskAssessmentStartDate']].max(axis=1)
    df_RA['FinalRiskLevel_High'] = 0
    df_RA['FinalRiskLevel_Medium/Moderate'] = 0
    df_RA['FinalRiskLevel_VeryHigh'] = 0
    df_RA['FinalRiskLevel_Low'] = 0
    df_RA['FinalRiskLevel_Missing/NA'] = 0
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
        'FinalRiskLevel_Missing/NA': 'max'})

    df = df.reset_index()
    df['max_recorded_date'] = df['max_recorded_date'].dt.strftime("%Y-%m")+'-01'
    df = df.rename(columns={'max_recorded_date': 'month'})
    
    return df

def read_Alternate_RISK_repots_data(inputDir, file):
    print('>>>Reading the data for Alternate RISK repots ... ' )

    Alternate_Risk_reports_columns=['PPN', 'ContactStartDateDate','AlternateAssessmentStartDate','AlternateAssessmentRiskLevel']
    df_RA = pd.read_csv(inputDir+file,usecols=Alternate_Risk_reports_columns, parse_dates=['ContactStartDateDate'], infer_datetime_format=True)

    df_RA.RiskAssessmentStartDate=pd.to_datetime(df_RA.RiskAssessmentStartDate, format='%m/%d/%Y')
    df_RA['max_recorded_date'] = df_RA[['ContactStartDateDate','RiskAssessmentStartDate']].max(axis=1)
    df_RA['AltRiskLevel_High'] = 0
    df_RA['AltRiskLevel_Medium/Moderate'] = 0
    df_RA['AltRiskLevel_VeryHigh'] = 0
    df_RA['AltRiskLevel_Low'] = 0
    df_RA['AltRiskLevel_Missing/NA'] = 0
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
        'AltRiskLevel_Missing/NA': 'max'})

    df = df.reset_index()
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
  
def read_active_case_data(inputDir,file):
    print('>>>Reading the data for active cases ... ' )

    active_case_columns=['PPN','ContactStartDateDate','CaseEndDateDate','RiskAssessmentStartDate'
          , 'FieldAssessmentStartDateDate'
          ,'SafetyAssessmentStartDate',
           'HelplineAssessmentStartDate']
    df = pd.read_csv(inputDir+file,usecols=active_case_columns, parse_dates=['ContactStartDateDate'], infer_datetime_format=True)
    df['HelplineAssessmentStartDate']=pd.to_datetime(df['HelplineAssessmentStartDate'], format='%m/%d/%Y')
    df['CaseEndDateDate']=pd.to_datetime(df['CaseEndDateDate'], format='%m/%d/%Y')
    df['SafetyAssessmentStartDate']=pd.to_datetime(df['SafetyAssessmentStartDate'], format='%m/%d/%Y')
    df['FieldAssessmentStartDateDate']=pd.to_datetime(df['FieldAssessmentStartDateDate'], format='%m/%d/%Y')
    df['RiskAssessmentStartDate']=pd.to_datetime(df['RiskAssessmentStartDate'], format='%m/%d/%Y')
    df['max_recorded_date']=df[['ContactStartDateDate','CaseEndDateDate','RiskAssessmentStartDate'
          , 'FieldAssessmentStartDateDate'
          ,'SafetyAssessmentStartDate',
           'HelplineAssessmentStartDate']].max(axis=1)

    df=df.drop(columns=['CaseEndDateDate','RiskAssessmentStartDate'
          , 'FieldAssessmentStartDateDate'
          ,'SafetyAssessmentStartDate',
           'HelplineAssessmentStartDate'],axis=1)


    return df

def aggregate_person_month_active_case(df):
    print('>>>Aggregating person-month active cases ... ' )
    final = df.merge(df.apply(lambda s: pd.date_range(str(s.ContactStartDateDate)[:7], s.max_recorded_date, freq='MS'), 1)
                     .explode()
                     .rename('month')
                     .dt.strftime("%Y-%m"),
                     left_index=True,
                     right_index=True)
    del df
    final['ActiveCase']=1
    final =final.drop(columns=["ContactStartDateDate", "max_recorded_date"], axis=1)
    final =final.drop_duplicates()
    final['month'] = final['month'].apply(lambda x:x+'-01')

    return final
  
  
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

    df_Safety=read_Safety_repots_data(inputDir, file)
    df_Safety = aggregate_person_month_Safety(df_Safety)
    df_merged=merge_full_join(df_merged,df_Safety)
    del df_Safety

    df_Final_RISK=read_Final_RISK_repots_data(inputDir, file)
    df_Final_RISK = aggregate_person_month_Final_RISK(df_Final_RISK)
    df_Alt_RISK=read_Final_Alternate_repots_data(inputDir, file)
    df_Alt_RISK = aggregate_person_month_Alternate_RISK(df_Alt_RISK)
    df_RISK =  summorize_person_month_Risk(df_Final_RISK , df_Alt_RISK)
    df_merged=merge_full_join(df_merged,df_RISK)
    del df_RISK

    df_active_case = read_active_case_data(inputDir, file)
    df_active_case =  aggregate_person_month_active_case(df_active_case = )
    df_merged=merge_full_join(df_merged,df_active_case)
    del df_active_case

    #save the final csv file
    df_merged.to_csv(outputDir+file , index= False)






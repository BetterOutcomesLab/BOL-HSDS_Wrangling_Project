import pandas as pd
import numpy as np

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/LEGAL_AID/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/LEGAL_AID/"



def wrangle_minor_assist_sensitive_data():

    #read data
    col=['PPN',
            'Service_date',
            'Law_type',
            'Minor_assistance_services' 
            ]
            
    df = pd.read_csv(inputDir + 'minor_assist_sensitive.csv',usecols=col,parse_dates=['Service_date'], infer_datetime_format=True)
    
    #clean and adjust data
    df=df.drop_duplicates()
    df.loc[df.Law_type=='CIVIL LAW','Law_type']='Civil'
    df.loc[df.Law_type=='FAMILY LAW','Law_type']='Family'
    df.loc[df.Law_type=='CRIMINAL LAW','Law_type']='Criminal'
       
    #aggregate on person month level
    df = df.groupby(['PPN',pd.Grouper(key='Service_date', freq="M"), 'Law_type' ]).agg({'Minor_assistance_services':'sum'}).reset_index()
    df['Service_date'] = df.Service_date.dt.strftime("%Y-%m")+'-01'
    
    
    df = pd.pivot(df, ['PPN','Service_date'] , ['Law_type'] , ['Minor_assistance_services']).reset_index()
    df.columns = ['ppn' , 'month' , 'CivilMinorAssist' , 'CriminalMinorAssist' , 'FamilyMinorAssist'  ]
    
    
    #save the output
    df.to_csv(outputDir+ 'minor_assist_sensitive.csv', index=False)

def wrangle_inhouse_duty_sensitive_data():

    #read data
    col=['PPN',
            'Service_date',
            'Law_type',
            'Inhouse_duty_services']
            
    df = pd.read_csv(inputDir + 'inhouse_duty_sensitive.csv',usecols=col,parse_dates=['Service_date'], infer_datetime_format=True)
    
    #clean and adjust data
    df=df.drop_duplicates()
    df.loc[df.Law_type=='CIVIL LAW','Law_type']='Civil'
    df.loc[df.Law_type=='FAMILY LAW','Law_type']='Family'
    df.loc[df.Law_type=='CRIMINAL LAW','Law_type']='Criminal'


    #aggregate on person month level
    df = df.groupby(['PPN',pd.Grouper(key='Service_date', freq="M"), 'Law_type' ]).agg({'Inhouse_duty_services':'sum'}).reset_index()
    df['Service_date'] = df.Service_date.dt.strftime("%Y-%m")+'-01'
    
   
    df = pd.pivot(df, ['PPN','Service_date'] , ['Law_type'] , ['Minor_assistance_services']).reset_index()
    df.columns = ['ppn' , 'month' , 'CivilInhouseDuty' , 'CriminalInhouseDuty' , 'FamilyInhouseDuty'  ]


    # save the output
    df.to_csv(outputDir + 'inhouse_duty_sensitive.csv', index=False)

def wrangle_grants_of_aid_sensitive_data():

    #read data
    col=['PPN',
        'Approval_date',
        'Law_type',
        'Applications_granted']
        
    df = pd.read_csv(inputDir + 'grants_of_aid_sensitive.csv',usecols=col,parse_dates=['Approval_date'], infer_datetime_format=True)

    #clean and adjust data
    df=df.drop_duplicates()
    df.loc[df.Law_type=='CIVIL LAW','Law_type']='Civil'
    df.loc[df.Law_type=='FAMILY LAW','Law_type']='Family'
    df.loc[df.Law_type=='CRIMINAL LAW','Law_type']='Criminal'

    #aggregate on person month level
    df = df.groupby(['PPN',pd.Grouper(key='Service_date', freq="M"), 'Law_type' ]).agg({'Applications_granted':'sum'}).reset_index()
    df['Service_date'] = df.Service_date.dt.strftime("%Y-%m")+'-01'
    
    
    df = pd.pivot(df, ['PPN','Service_date'] , ['Law_type'] , ['Minor_assistance_services']).reset_index()
    df.columns = ['ppn' , 'month' , 'CivilApplicationsGranted' , 'CriminalApplicationsGranted' , 'FamilyApplicationsGranted'  ]

    # save the output
    df.to_csv(outputDir + 'grants_of_aid_sensitive.csv', index=False)
def wrangle_inhouse_advice_sensitive_data():

    #read data
    bol_col=['PPN',
            'Service_date',
            'Law_type',
            'Advice_services']
            
    df = pd.read_csv(inputDir + 'inhouse_advice_sensitive.csv',usecols=bol_col,parse_dates=['Service_date'], infer_datetime_format=True)
    
    #clean and adjust data
    df=df.drop_duplicates()
    df.loc[df.Law_type=='CIVIL LAW','Law_type']='Civil'
    df.loc[df.Law_type=='FAMILY LAW','Law_type']='Family'
    df.loc[df.Law_type=='CRIMINAL LAW','Law_type']='Criminal'

    #aggregate on person month level
    df = df.groupby(['PPN',pd.Grouper(key='Service_date', freq="M"), 'Law_type' ]).agg({'Advice_services':'sum'}).reset_index()
    df['Service_date'] = df.Service_date.dt.strftime("%Y-%m")+'-01'
    
    
    df = pd.pivot(df, ['PPN','Service_date'] , ['Law_type'] , ['Minor_assistance_services']).reset_index()
    df.columns = ['ppn' , 'month' , 'CivilInhouseAdvice' , 'CriminalInhouseAdvice' , 'FamilyInhouseAdvice'  ]

    #save the output
    df.to_csv(outputDir+ 'inhouse_advice_sensitive.csv', index=False)



def wrangle_ela_sensitive_data():

    #read data
    col=['PPN',
        'Approval_date',
        'ELA_approvals'
             ]
    df = pd.read_csv(inputDir + 'ela_sensitive.csv',usecols=bol_col,parse_dates=['Approval_date'], infer_datetime_format=True)

    #clean and adjust data
    df=df.drop_duplicates()

    #aggregate on person month level
    df = df.groupby(['PPN',pd.Grouper(key='Approval_date', freq="M") ]).agg({'ELA_approvals':'sum'}).reset_index()
    df['Approval_date'] = df.Approval_date.dt.strftime("%Y-%m")+'-01'
    df=df.rename(columns={'PPN':'ppn',
                  'Approval_date':'month',
                  'ELA_approvals':'ELA_Approvals'
                  })

    #save the output
    df.to_csv(outputDir+ 'ela_sensitive.csv', index=False)



if __name__ == '__main__':


    wrangle_minor_assist_sensitive_data()
    wrangle_inhouse_duty_sensitive_data()
    wrangle_grants_of_aid_sensitive_data()
    wrangle_inhouse_advice_sensitive_data()
    wrangle_ela_sensitive_data()

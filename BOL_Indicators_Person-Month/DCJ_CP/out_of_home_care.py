import pandas as pd
import numpy as np

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_CP/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_CP/"


file='PRJ_2021_40_3_OOHC_Longitudinal_File_30JUNE_SENSITIVE.csv'


def read_agg_oohc_placement_data(inputDir,file):
    print('>>>Reading the data for oohc placement ... ' )

    oohc_placement_columns = ['PPN',
                              'PriorityPlacementProviderGrouped',
                              'ActualPlacementStartDate',
                              'ActualPlacementEndDate',
                              'ActualPlacementTypeGrouped'  
                              
                              ]
    df = pd.read_csv(inputDir+file,usecols=oohc_placement_columns, parse_dates=['ActualPlacementStartDate',
                              'ActualPlacementEndDate'], infer_datetime_format=True)
    #If 'ActualPlacementEndDate' is not stated in a record, we consider the case to be an ongoing service and set the data extract date as the value for 'ActualPlacementEndDate'.
    df['ActualPlacementEndDate'] = df['ActualPlacementEndDate'].fillna('30-06-2020')
     
    #expand datafram with binary columns for values in 'PriorityPlacementProviderGrouped'
    PriorityPlacementProviderGrouped_cols = ['PriorityPlacementProviderGrouped'+'_'+ col for col in pd.get_dummies(df['PriorityPlacementProviderGrouped']).columns]
    df = pd.get_dummies(df, columns = ['PriorityPlacementProviderGrouped'])
    
    #expand datafram with binary columns for values in 'ActualPlacementTypeGrouped'
    ActualPlacementTypeGrouped_cols = ['ActualPlacementTypeGrouped'+'_'+ col for col in pd.get_dummies(df['ActualPlacementTypeGrouped']).columns]
    df = pd.get_dummies(df, columns = ['ActualPlacementTypeGrouped'])   


    df = df.merge(df.apply(lambda s: pd.date_range(str(s.ActualPlacementStartDate)[:7], s.ActualPlacementEndDate, freq='MS'), 1)
                     .explode()
                     .rename('month')
                     .dt.strftime("%Y-%m")
                     ,
                     left_index=True,
                     right_index=True)
                     

    #drop date columns
    df = df.drop(columns=["ActualPlacementStartDate", "ActualPlacementEndDate"], axis=1).drop_duplicates()
        
    #form the aggregation rules for group by 
    aggregation_rules = {}
    for list in [PriorityPlacementProviderGrouped_cols,ActualPlacementTypeGrouped_cols,ActualPlacementPurpose_cols]:
        aggregation_rules.update({col : 'max' for col in list})                   
    
    #aggregate the data frame by unique ppn and month values 
    df = df.groupby(['PPN' , 'month']).agg(aggregation_rules).reset_index()
   
    df['ActiveOOHCPLC']=1

    #handle missing values and minimize memory use 
    for col in df.columns[2:]:
        df[col] = df[col].fillna(0).astype(np.int8)
        
    return df
    
def read_agg_oohc_care_data(inputDir,file):
    print('>>>Reading the data for oohc care giver category ... ' )

    oohc_care_columns = ['PPN',
                          'CareCategoryStartDate',
                          'CareCategoryEndDate',
                          'TypeofCaregiverGrouped',
                          'ExitCareType'
                         ]
    df = pd.read_csv(inputDir+file,usecols=oohc_care_columns, parse_dates=['CareCategoryStartDate',
                              'CareCategoryEndDate'], infer_datetime_format=True)
                              
    #If 'CareCategoryEndDate' is not stated in a record, we consider the case to be an ongoing service and set the data extract date as the value for 'CareCategoryEndDate'.
    df['CareCategoryEndDate'] = df['CareCategoryEndDate'].fillna('30-06-2020')
    
    #expand datafram with binary columns for values in 'TypeofCaregiverGrouped'
    TypeofCaregiverGrouped_cols = ['TypeofCaregiverGrouped'+'_'+ col for col in pd.get_dummies(df['TypeofCaregiverGrouped']).columns]
    df = pd.get_dummies(df, columns = ['TypeofCaregiverGrouped'])
 

    #expand datafram with binary columns for values in 'ExitCareType'
    ExitCareType_cols = ['ExitCareType'+'_'+ col for col in pd.get_dummies(df['ExitCareType']).columns]
    df = pd.get_dummies(df, columns = ['ExitCareType']) 

    #capture the last month of the carecategory episode corresponded to the ExitCareType value
    df_exit_reason = df[['PPN','CareCategoryEndDate']+ExitCareType_cols].drop_duplicates()
    df_exit_reason['month'] = df_exit_reason['CareCategoryEndDate'].apply(lambda s : str(s)[:7])
    df_exit_reason = df_exit_reason.drop(columns=['CareCategoryEndDate'],axis=1)
    aggregation_rules = {}
    for col in ExitCareType_cols:
        aggregation_rules.update({col : 'max'})
    df_exit_reason = df_exit_reason.groupby(['PPN' , 'month']).agg(aggregation_rules).reset_index()
    
    
    df = df[['PPN','CareCategoryStartDate','CareCategoryEndDate']+TypeofCaregiverGrouped_cols].drop_duplicates()

    df = df.merge(df.apply(lambda s: pd.date_range(str(s.CareCategoryStartDate)[:7], s.CareCategoryEndDate, freq='MS'), 1)
                     .explode()
                     .rename('month')
                     .dt.strftime("%Y-%m")
                     ,
                     left_index=True,
                     right_index=True).drop(columns=["CareCategoryStartDate", "CareCategoryEndDate"], axis=1).drop_duplicates()


    aggregation_rules = {}
    for col in TypeofCaregiverGrouped_cols:
        aggregation_rules.update({col : 'max'})

    df = df.groupby(['PPN','month']).agg(aggregation_rules).reset_index()
    
    
    df['ActiveCareCategory'] = 1
    

    #merge in the last month and exit reason captured in df_exit_reason data frame previously  
    df = pd.merge(df,df_exit_reason, on=['PPN','month'], how='left')
    
    return df
    

def merge_full_join(df1,df2):
    df_merge=pd.merge(df1, df2, on=['PPN', 'month'], how='outer')
    for col in [i for i in df_merge.columns if i not in ['PPN', 'month'] ]:
        df_merge[col]=df_merge[col].fillna(0).astype(np.int8)
    return df_merge


if __name__ == '__main__':


    df_oohc_placement = read_agg_oohc_placement_data(inputDir,file)
    df_oohc_care = read_agg_oohc_care_data(inputDir,file)
    df = merge_full_join(df_oohc_placement, df_oohc_care)

    df.to_csv(outputDir+file, index=False)



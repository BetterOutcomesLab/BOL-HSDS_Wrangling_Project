import pandas as pd
import numpy as np


#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_CP/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_CP/"

#to be completes ... 
  
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






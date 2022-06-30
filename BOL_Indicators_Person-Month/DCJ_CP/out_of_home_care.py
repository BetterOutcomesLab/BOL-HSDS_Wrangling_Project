import pandas as pd
import numpy as np


#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/input/DCJ_CP/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/output/DCJ_CP/"

file = 'PRJ_2021_04_3_OOHC_Longitudinal_File_30JUNE_SENSITIVE.csv'


if __name__ == '__main__':


    df_oohc_placement = read_agg_oohc_placement_data(inputDir,file)
    df_oohc_care = read_agg_oohc_care_data(inputDir,file)
    df=merge_full_join(df_oohc_placement, df_oohc_care)
    df_oohc_LegalOrder = read_agg_oohc_LegalOrder_data(inputDir,file)
    df=merge_full_join(df, df_oohc_LegalOrder)
    print(df.columns)
    df=df[reorder_col]
    df.iloc[:,2:]=df.iloc[:,2:].astype(np.int8)
    # df.month=df.month.astype(str)
    df.to_csv(outputDir+file, index=False)




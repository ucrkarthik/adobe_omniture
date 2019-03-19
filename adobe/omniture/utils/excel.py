from typing import List, Tuple
import pandas as pd
from datetime import datetime
import shutil, os

def create_excel_spreadsheet(dfs: List[Tuple[str, pd.DataFrame]], target_path: str) -> None:
    """
    Takes a list of tuples (report names, pandas dataframes) and creates an xlsx file, where each tuple is
    a seperate sheet.
    :param dfs: list of tuples - names and pandas dataframes, e.g. [('adherence', adherence_df), ...]
    :param s3_path: desired s3 location for the xlsx file
    :return:
    """
    local_filename = datetime.now().strftime("%Y-%m-%d") + '_SearchKeywordPerformance.xlsx'
    excel_writer = pd.ExcelWriter(local_filename, engine='xlsxwriter')
    for i, tup in enumerate(dfs):
        tup[1].to_excel(excel_writer, sheet_name=tup[0])
    excel_writer.save()

    if("s3://" in target_path):
        os.system(f"aws s3 cp {local_filename} {target_path}/{local_filename} --sse --acl bucket-owner-full-control")
    else:
        shutil.move(local_filename, target_path+"/"+local_filename)  # add source dir to filename


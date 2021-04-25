import pandas as pd
import numpy as np

def postprocess(test_preds_df, test_df):
    submit_df = pd.DataFrame()
    submit_df['お仕事No.'] = test_df['お仕事No.']
    submit_df['応募数 合計'] = test_preds_df.mean(axis=1)
    return submit_df
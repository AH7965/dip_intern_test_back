import os.path as osp
import pandas as pd
import numpy as np
from parameter import *

from tqdm import tqdm

print("load model start")
models = pd.read_pickle(osp.join(OUTPUT_DIR, 'model.pkl'))

print("load model end")
def estimate(test_encoded, models=models):
    model_key = "lgb"
    test_preds_df = test_encoded.drop(columns=test_encoded.columns)

    for index, seed in enumerate(tqdm([42, 21, 63, 128, 96])):
        name = f"{model_key}_preds_{seed}"
        if name not in test_preds_df.columns:
            test_preds_df[name] = 0
        name = f"{model_key}_preds_{seed}"
        models_tmp = models[(index)*5:(index+1)*5]
        pred = np.array([models_tmp[i].predict(test_encoded, num_iteration=models_tmp[i].best_iteration) for i in range(len(models_tmp))])
        pred = pred.mean(axis=0)
        test_preds_df[name] = pred
    return test_preds_df


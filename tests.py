from TikTokDataLoader import TikTokDataLoader
from SVM import *
import os
import pandas as pd

if __name__ == "__main__":
    data = TikTokDataLoader.read_full_data(5)
    model=train_svm(data)
    #Get a Human account to test
    test_dir = [d for d in os.listdir("data") if d.startswith("(Human)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)
    df_main, following_dfs_dict = tiktok_data_loader.load_data()
    predict(df_main,model,5.0)
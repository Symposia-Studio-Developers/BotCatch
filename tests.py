from TikTokDataLoader import TikTokDataLoader
import os
import pandas as pd

if __name__ == "__main__":
    
    test_dir = [d for d in os.listdir("data") if d.startswith("(Bot)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)

    df_main, following_dfs_dict = tiktok_data_loader.load_data()
    print(df_main.head())
    print(f"There are {len(following_dfs_dict)} following dataframes")
    print(following_dfs_dict[1])
    print(following_dfs_dict[2])
import pandas as pd
import os
import concurrent.futures
from tqdm import tqdm


class TikTokDataLoader:
    def __init__(self, dir_path, on_bad_lines="skip"):
        self.dir_path = dir_path
        self.on_bad_lines = on_bad_lines
        self.main_account = []
        self.following_list = []
        self.df_main = pd.DataFrame()
        self.following_dfs_dict = []
            # [{'nodeName': 'account1', 'following_df': df1}
            # ,..., {'nodeName': 'accountN', 'following_df': dfN}
            # ]

    def _read_csv(self, csv_name):
        return pd.read_csv(os.path.join(self.dir_path, csv_name), on_bad_lines=self.on_bad_lines)
    
    def _read_following_list_csv(self, csv_name):
        df = self._read_csv(csv_name)    
        return {
            'nodeName': csv_name[:-14],
            'following_df': df
        }
    def _get_files(self):
        csv_list = os.listdir(self.dir_path)
        self.main_account = [csv for csv in csv_list if csv.endswith("_Follower.csv")]
        self.following_list = [csv for csv in csv_list if csv.endswith("_Following.csv")]
        assert len(self.main_account) == 1, "checks, no/too many main_account found"

    def load_data(self) -> (pd.DataFrame, list):
        self._get_files()
        self.df_main = self._read_csv(self.main_account[0])

        with concurrent.futures.ThreadPoolExecutor() as executor:
            self.following_dfs_dict = list(tqdm(executor.map(self._read_following_list_csv, self.following_list), total=len(self.following_list), desc="Reading Following CSVs"))
        return self.df_main, self.following_dfs_dict
    

if __name__ == "__main__":
    
    test_dir = [d for d in os.listdir("data") if d.startswith("(Bot)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)

    df_main, following_dfs_dict = tiktok_data_loader.load_data()
    print(df_main.head())
    print(f"There are {len(following_dfs_dict)} following dataframes")
    print(following_dfs_dict[1])

        
    

import pandas as pd
import os
import concurrent.futures
from tqdm import tqdm
from .constants import ATTRIBUTE_TYPES
import warnings

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


    def _enforce_df_col_types(self, df):

        _bool_map = {
                    "True": True,
                    "False": False,
                    "0": False,
                    "1": True
        }

        for col in df.columns:
            if col not in ATTRIBUTE_TYPES:
                warnings.warn(f"Column {col} not in ATTRIBUTE_TYPES, skipping")
                continue
            if ATTRIBUTE_TYPES[col] == bool:
                df[col] = df[col].apply(lambda x: _bool_map[x] if x in _bool_map else bool(x))

            elif col == "stitchSetting": # stitchSetting fixes: mixed bool and int type
                # str -> bool
                # bool -> int
                # int -> int
                ad_hoc_fix = lambda x: _bool_map[x] if x in _bool_map else int(x) if type(x) == bool else int(x)
                df[col] = df[col].apply(ad_hoc_fix)
            else:
                df[col] = df[col].astype(ATTRIBUTE_TYPES[col])
        return df
    
    def _read_csv(self, csv_name):
        return pd.read_csv(
            os.path.join(self.dir_path, csv_name),
            on_bad_lines=self.on_bad_lines
        ).pipe(self._enforce_df_col_types)
    
    def _read_following_list_csv(self, csv_name):
        df = self._read_csv(csv_name)    
        return {
            'nodeName': csv_name[:-14],
            'df': df
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
        return (
                {'nodeName':self.main_account[0][:-13], 'df': self.df_main},
                self.following_dfs_dict
        )
    

if __name__ == "__main__":
    
    test_dir = [d for d in os.listdir("data") if d.startswith("(Bot)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)

    df_main, following_dfs_dict = tiktok_data_loader.load_data()
    df_main = df_main['df']
    print(df_main.head())
    print(f"There are {len(following_dfs_dict)} following dataframes")
    print(following_dfs_dict[1])

        
    

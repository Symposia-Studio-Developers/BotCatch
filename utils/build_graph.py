import networkx as nx
import tqdm
from .TikTokDataLoader import TikTokDataLoader

class TikTokGraphBuilder:
    DROPS_COLS = [
            'friendCount',
            'ftc',
            'isADVirtual',
            'relation',
            'ttSeller',  # * -> not spam if True
            'verified',  # * -> not spam if True
    ]
    def __init__(self, in_memory=False, dataloader: TikTokDataLoader = None):
        self.G = nx.DiGraph()
        self.in_memory = in_memory
        self.dataloader = dataloader
        self.main_account_id = None

    def _remove_noinfo_features(self, df):
        if not any([c in df.columns for c in self.DROPS_COLS]):
            return df
        return df.copy().drop(columns=self.DROPS_COLS)

    def add_main_account(self, main_account_dict):
        main_account_id = main_account_dict["nodeName"]
        self.main_account_id = main_account_id
        self.G.add_node(main_account_id,
                        **{
                            'is_main': True,
                            'is_secondary': False,
                        })
        # build main account followers
        for _, row in tqdm.tqdm(self._remove_noinfo_features(main_account_dict['df']).iterrows(), total=len(main_account_dict["df"]),
                                desc="Building main account followers"):
            follower_id = row["secUid"]
            self.G.add_node(follower_id, **{
                'is_main': False,
                'is_secondary': False,
                **row.to_dict(),
            })
            self.G.add_edge(main_account_id, follower_id)

    def add_secondary_followings(self, following_dfs_dict):
        # build secondary account following list
        for following_dict in tqdm.tqdm(following_dfs_dict, total=len(following_dfs_dict),
                                        desc="Building secondary account following list"):
            for _, row in self._remove_noinfo_features(following_dict['df']).iterrows():
                parent_id = following_dict['nodeName']
                following_id = row["secUid"]
                if following_id == self.main_account_id:
                    continue
                if following_id not in self.G.nodes:
                    self.G.add_node(following_id, **{
                        'is_main': False,
                        'is_secondary': True,
                        **row.to_dict(),
                    })
                self.G.add_edge(parent_id, following_id)

    def build_graph(self, main_account_dict=None, following_dfs_dict=None):
        if self.dataloader and not (main_account_dict and following_dfs_dict):
            # try to load data from dataloader first
            main_account_dict = getattr(self.dataloader, "main_account_dict", None)
            following_dfs_dict = getattr(self.dataloader, "following_dfs_dict", None)
            
            if not (main_account_dict and following_dfs_dict):
                main_account_dict, following_dfs_dict = self.dataloader.load_data()
                
        elif not (main_account_dict and following_dfs_dict):
            raise ValueError("Please provide either dataloader or main_account_dict and following_dfs_dict.")
        self.add_main_account(main_account_dict)
        self.add_secondary_followings(following_dfs_dict)
        
        if self.in_memory:
            self.graph = self.G
        else:
            graph_to_return = self.G
            self.G = None  # clear the reference
            return graph_to_return


    def get_graph(self):
        if hasattr(self, 'graph'):
            return self.graph
        else:
            raise ValueError("Graph not saved in memory. Please set in_memory=True during initialization or build the graph first.")

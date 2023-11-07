import networkx as nx
from .build_graph import TikTokGraphBuilder
import numpy as np
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

class TikTokFeatureExtractor:
    def __init__(self, graph_builder: TikTokGraphBuilder = None,
                 agg_func: dict = None):
        self.graph_builder = graph_builder
        self.agg_func = agg_func
        """
        Example agg_func:
            {
                "heart": ["sum", "mean", ..., custom_func],
                "start_time": ["min", "max", ..., custom_func],
                ...
            }
        """
        self._check_agg_func()
        
        self.features = None
        self.G: nx.DiGraph = nx.DiGraph()
    
        self._supported_agg_func = ["sum", "mean", "min", "max", "std"]
        self._default_agg_method = "mean"
        self._debug = False

    def set_debug_mode(self, debug: bool):
        self._debug = debug

    def _check_agg_func(self):
        if self.agg_func is None:
            self.agg_func = dict()
        else:
            for k, v in self.agg_func.items():
                if type(v) != list:
                    self.agg_func[k] = [v]

    def _get_features(self):
        main_node = self.graph_builder.main_account_id
        succ_node = list(self.G.successors(main_node))[0]
        # use the first child node of main node to get features
        self.features = self.G.nodes[succ_node].keys()
        self.features = list(set(self.features) - set(["is_main", "is_secondary"]))
    
    def _agg(self, data: np.ndarray, agg_func):
        '''
        data: np.ndarray
        agg_func: str (np supported agg func) or callable
        '''
        # if data is empty, return np.nan
        if len(data) == 0:
            return np.nan
        if agg_func in self._supported_agg_func:
            return getattr(data, agg_func)()
        else:
            return agg_func(data)
    def _default_agg(self, data: np.ndarray):
        if np.issubdtype(data.dtype, np.integer) or np.issubdtype(data.dtype, np.floating) or np.issubdtype(data.dtype, np.bool_):
            return self._agg(data, self._default_agg_method)
        else:
            return np.nan

    def _feature_extractor(self, node: str, dropna: bool = True):
        result_dict = {f: {'data': [],} for f in self.features}
        for succ in self.G.successors(node):
            for k, v in self.G.nodes[succ].items():
                if k in self.features:
                    if dropna and pd.isna(v):
                        continue
                    result_dict[k]['data'].append(v)

        ''' ##########################################
        rows in `main_account`_follower.csv and *_following.csv
        are not 1-to-1, some followers do not have following list (csv)
        '''
        if self._debug:
            for k, v in result_dict.items():
                if not v['data']:
                    print(f"Warning: {node} has no data for feature {k}")
        '''
        ''' ##########################################

        for col, data in result_dict.items():
            tmp = {}
            if col in self.agg_func:
                for agg in self.agg_func[col]:
                    agg_name = agg if type(agg) == str else agg.__name__
                    tmp[agg_name] = self._agg(np.array(data['data']), agg)
            else: # fallback to default agg if not specified
                tmp[self._default_agg_method] = self._default_agg(np.array(data['data']))
            
            result_dict[col] = tmp

        return result_dict
    
    def extract_features(self, 
                        agg_func: dict = None, 
                        dropna: bool = True,
                        return_df: bool = True):
        if agg_func:
            self.agg_func = agg_func
            self._check_agg_func()
        
        self.G = self.graph_builder.build_graph()
        self._get_features()
        main_node = self.graph_builder.main_account_id

        result_dicts = [
            {**self._feature_extractor(n, dropna=dropna),
             'node': n
            } for n in self.G.successors(main_node)
        ]

        return self._to_dataframe(result_dicts) if return_df else result_dicts

    def _to_dataframe(self, result_dicts):
        return pd.json_normalize(result_dicts)
        







if __name__ == "__main__":
    pass
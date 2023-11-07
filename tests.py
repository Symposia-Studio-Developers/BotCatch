from utils.TikTokDataLoader import TikTokDataLoader
from utils.build_graph import TikTokGraphBuilder
from utils.featurizer import TikTokFeatureExtractor
import os
import pandas as pd
import numpy as np

if __name__ == "__main__":
    
    test_dir = [d for d in os.listdir("data") if d.startswith("(Bot)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)
    tiktok_graph_builder = TikTokGraphBuilder(dataloader=tiktok_data_loader, in_memory=True)
    tiktok_feature_extractor = TikTokFeatureExtractor(graph_builder=tiktok_graph_builder)
    #tiktok_feature_extractor.set_debug_mode(False)
    result = tiktok_feature_extractor.extract_features(agg_func={
        'heart': ['sum', 'mean', 'std'],
        'start_time': ['min', 'max', 'std'],
        'end_time': ['min', 'max', 'std'],
        'followerCount': ['sum', 'mean', 'std'],
        'followingCount': ['sum', 'mean', 'std'],
        'videoCount': ['sum', 'mean', 'std'],
        'diggCount': ['sum', 'mean'],
        'commentSetting': ['sum'],
    })

    result

    def custom_func(x: np.ndarray):
        return x.mean() + x.std()
    
    result = tiktok_feature_extractor.extract_features(agg_func={
        'heart': ['sum', 'mean', 'std', custom_func],
        'start_time': ['min', 'max', 'std'],
        'end_time': ['min', 'max', 'std'],
        'followerCount': ['sum', 'mean', 'std'],
        'followingCount': ['sum', 'mean', 'std'],
        'videoCount': ['sum', 'mean', 'std'],
        'diggCount': ['sum', 'mean', custom_func],
        'commentSetting': ['sum']
    })

    result
    # G = tiktok_graph_builder.build_graph()
    # succ_nodes = list(G.successors(tiktok_graph_builder.main_account_id))
    # print(len(succ_nodes))
    # print(G.nodes[succ_nodes[0]])
    # print(G.nodes[succ_nodes[0]].keys())
    # print(tiktok_data_loader.cols_all_occurences)

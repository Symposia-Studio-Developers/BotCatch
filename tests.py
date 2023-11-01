from utils.TikTokDataLoader import TikTokDataLoader
from utils.build_graph import TikTokGraphBuilder
import os
import pandas as pd

if __name__ == "__main__":
    
    test_dir = [d for d in os.listdir("data") if d.startswith("(Bot)")][0]
    test_dir = os.path.join("data", test_dir)
    tiktok_data_loader = TikTokDataLoader(dir_path=test_dir)
    tiktok_graph_builder = TikTokGraphBuilder(dataloader=tiktok_data_loader)
    G = tiktok_graph_builder.build_graph()
    succ_nodes = list(G.successors(tiktok_graph_builder.main_account_id))
    print(len(succ_nodes))
    print(G.nodes[succ_nodes[0]])
    print(G.nodes[succ_nodes[0]].keys())
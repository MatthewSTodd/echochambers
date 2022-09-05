from preprocessing.ops_on_raw_data import check_directory_absence
from preprocessing.utilities import (get_only_date, get_metadata,
                                    clean, manage_and_save, add_edge,
                                    create_multi_graph)
#from preprocessing.ops_sentiment_affin import add_sentiment
from preprocessing.ops_sentiment_vader import add_sentiment
from preprocessing.topic_modelling import add_topic
import networkx as nx 
import pandas as pd
from tqdm import tqdm
import os
import ast
import time

def graph_ops():
    covid_graph()
    add_sentiment()
    # add_topic()


def covid_graph():
    path = os.path.join(os.getcwd(), 'data', 'corona_virus')
    if not os.path.exists(os.path.join(path, 'Graph')):
        os.mkdir(os.path.join(path, 'Graph'))
    build_covid_graph()
    # TODO reinstate this
    #add_meta(path)


def build_covid_graph():
    final_data_path = os.path.join(os.getcwd(), 'data', 'corona_virus', 'final_data', 'Final_data.csv')
    df = pd.read_csv(final_data_path, usecols=[
        'username', 'favorites', 'retweets', '@mentions', 'geo', 'text_con_hashtag'])
    df.dropna(axis='index', how='all', subset=['text_con_hashtag'], inplace=True)
    df = df.rename(columns={'@mentions' : 'mentions'})
    g_dg = nx.DiGraph()
    g_g = nx.Graph()

    for _, row in tqdm(df.iterrows(), desc="Rows processed"):
        if row.mentions == 'self':
            g_dg = add_edge(
                g_dg, row.text_con_hashtag, 'covid', row.favorites, row.retweets, 0, row.username, row.username)
        else:
            mentions = row.mentions.split(',')
            for mention in mentions:
                mention = mention.strip()
                if mention != '' and mention != '@':
                    g_dg = add_edge(g_dg, row[5], 'covid', row[0], row[2], 0, row[3], mention)
                    g_g = add_edge(g_g, None, None, None, None, None, row[3], mention)

    g_dg.name = 'Starter covid Direct Graph'
    g_g.name = 'Starter covid Graph'

    graphs = [g_dg, g_g]
    manage_and_save(graphs)


def add_meta(path):
    G = nx.read_gml(os.path.join('Graph', 'Final_DiGraph_Covid.gml'))
    user_metadata_clean = get_metadata()

    not_included = 0
    for node in tqdm(G.nodes(), desc = " Node processed"):
        for column in user_metadata_clean.columns:
            try:
                G.nodes[node][column] = user_metadata_clean.loc[node][column]
            except:
                not_included += 1
                break
    print()
    print("TOTAL NUMBER OF NODES NOT LABELED:{:>10}".format(not_included))
    print("TOTAL NUMBER OF LABELED:          {:>10}".format(len(G) - not_included))
    write_path = os.path.join(path, 'Graph', 'Final_DiGraph_Covid_data.gml')
    nx.write_gml(G, write_path)

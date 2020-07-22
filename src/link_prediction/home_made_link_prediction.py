from controversy_detection.controversy_utilities import create_multi_graph
from link_prediction.link_pred_utilities import add_sentiment_boost
from controversy_detection.random_walks import top_out_degree

from networkx.algorithms.centrality import  betweenness_centrality

import random

import numpy as np

from networkx.classes.function import non_edges


def get_edges_to_add_degree(start_graph, com_type, opt=0):
    to_add = list()
    total_weight = list()
    for edge in start_graph.edges(data=True):
        total_weight.append(edge[2]['weight'])
        
    max_edges_to_add = int(sum(total_weight)*0.3)
    
    graph = create_multi_graph(start_graph, opt)
    
    top_com_0 = top_out_degree(graph, 25, 0)
    top_com_1 = top_out_degree(graph, 25, 1)
    
    for top_0 in top_com_0:
        for top_1 in top_com_1:
            if not(start_graph.has_edge(top_0, top_1)):
                to_add.append((top_0, top_1, 1))
            if len(to_add) > max_edges_to_add:
                break
    if opt == 1:
        edge_to_add = add_sentiment_boost(start_graph, to_add)
        to_add = sorted(edge_to_add, key=lambda tup: tup[2], reverse=True)
    
    return to_add


def get_edges_to_add_bet(start_graph, com_type, opt=0):
    
    node_high_bet_0 = list()
    node_high_bet_1 = list()
    mean_bet = list()
    total_weight = list()
    for edge in start_graph.edges(data=True):
        total_weight.append(edge[2]['weight'])
        
    max_edges_to_add = int(sum(total_weight)*0.3)
    
    edge_to_add = list()
    
    nodes_bet = betweenness_centrality(start_graph, k=int(len(start_graph)*0.6), weight='weight')
    
    for node in nodes_bet:
        mean_bet.append(nodes_bet[node])

    for node in nodes_bet:
        if nodes_bet[node] >= np.mean(mean_bet):
            if start_graph.nodes[node][com_type] == 0:
                node_high_bet_0.append((node, nodes_bet[node]))
            elif start_graph.nodes[node][com_type] == 1:
                node_high_bet_1.append((node, nodes_bet[node]))
    
    for node_0 in node_high_bet_0:
        for node_1 in node_high_bet_1:
            if not(start_graph.has_edge(node_0[0], node_1[0])):
                edge_to_add.append((node_0[0], node_1[0], node_0[1] + node_1[1]))

    sorted_edge = sorted(edge_to_add, key=lambda tup: tup[2], reverse=True)
    sorted_to_add = list()
    for edge in sorted_edge:
        if len(sorted_to_add) > max_edges_to_add:
            break  
        else:
            sorted_to_add.append(edge)

    if opt == 1:
        edge_to_add = add_sentiment_boost(start_graph, edge_to_add)
        sorted_to_add = sorted(sorted_to_add, key=lambda tup: tup[2], reverse=True)
    else:
        sorted_to_add = sorted(edge_to_add, key=lambda tup: tup[2], reverse=True)
    
    return sorted_to_add


def add_top_deg_to_normal(start_graph, com_type, opt=0):
    total_weight = list()
    graph = create_multi_graph(start_graph, opt)
    for edge in start_graph.edges(data=True):
        total_weight.append(edge[2]['weight'])
        
    
    top_com_0 = top_out_degree(graph, 25, 0)
    top_com_1 = top_out_degree(graph, 25, 1)
    
    max_edges_to_add = int(sum(total_weight)*0.3)
    
    to_add = list()
    
    for node in start_graph.nodes(data=True):
        if len(to_add) > max_edges_to_add:
            break  
        else:
            if node[0] not in top_com_0 and node[0] not in top_com_1:
                if node[1][com_type] == 0:
                    to_take = random.randint(0, len(top_com_1) - 1)
                    if not(start_graph.has_edge(node[0], top_com_1[to_take])):
                        to_add.append((node[0], top_com_1[to_take], 1))
                elif node[1][com_type] == 1:
                    to_take = random.randint(0, len(top_com_0) - 1)
                    if not(start_graph.has_edge(node[0], top_com_0[to_take])):
                        to_add.append((node[0], top_com_0[to_take], 1))     
    if opt == 1:
        to_add = add_sentiment_boost(start_graph, to_add)
        to_add = sorted(to_add, key=lambda tup: tup[2], reverse=True)
        
    return to_add
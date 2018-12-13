# this script file:
# 1) reads in the entry-exit network from the file in the argument
# 2) filters for the entry-exit types in the argument, and possibly nodes
# 3) calculates out-strength and weighted core-number
# 4) returns a network in Pajek for each split

import networkx as nx

def generate_pajek(G,teleport=None,name_list=None):
    """
    Modified from NetworkX source code: https://networkx.github.io/documentation/stable/_modules/networkx/readwrite/pajek.html#write_pajek
    """
    # write nodes with attributes
    yield '*vertices %s' % (G.order())
    first = True
    nodes = list(G)
    # make dictionary mapping nodes to integers
    nodenumber = dict(zip(nodes, range(1, len(nodes) + 1)))
    for n in nodes:
        na = G.node[n] if n in G.node else {}
        if first:
            na_header = [str(attr) for attr in na.keys()]
            if teleport:
                yield '#'+' '.join(['index','name','teleport'])+' '+' '.join(na_header)
            else:
                yield '#'+' '.join(['index','name'])+' '+' '.join(na_header)
            first = False
        if name_list:
            name = '_'.join([G.node[n][term] for term in name_list]+[n[0:4]])
        else:
            name = n
        if teleport:
            s = ' '.join(map(str,(nodenumber[n],name,G.node[n][teleport])))+' '+' '.join([str(na[attr]) for attr in na_header])
        else:
            s = ' '.join(map(str,(nodenumber[n],name)))+' '+' '.join([str(na[attr]) for attr in na_header])
        yield s
    # write edges with attributes
    if G.is_directed():
        yield '*arcs'
    else:
        yield '*edges'
    for u, v, edgedata in G.edges(data=True):
        d = edgedata.copy()
        value = d.pop('weight', 1.0)  # use 1 as default edge value
        s = ' '.join(map(str, (nodenumber[u], nodenumber[v], value)))
        yield s

def parse_pajek(lines):
    """
    Modified from NetworkX source code: https://networkx.github.io/documentation/stable/_modules/networkx/readwrite/pajek.html#read_pajek
    """
    # multigraph=False
    lines = iter([line.rstrip('\n') for line in lines])
    G = nx.DiGraph()  # are multiedges allowed in Pajek? assume yes
    labels = []  # in the order of the file, needed for matrix
    while lines:
        try:
            l = next(lines)
        except:  # EOF
            break
        if l.lower().startswith("*network"):
            try:
                label, name = l.split(None, 1)
            except ValueError:
                # Line was not of the form:  *network NAME
                pass
            else:
                G.graph['name'] = name
        elif l.lower().startswith("*vertices"):
            nodelabels = {}
            l, nnodes = l.split()
            for i in range(int(nnodes)):
                l = next(lines)
                if l.lower().startswith("#"):
                    attr = l.strip('#').split()
                    l = next(lines)
                splitline = l.split()
                id, label = splitline[0:2]
                labels.append(label)
                G.add_node(label)
                nodelabels[id] = label
                G.node[label]['id'] = id
                extra_attr = zip(attr[2:], splitline[2:])
                G.node[label].update(extra_attr)
        elif l.lower().startswith("*edges") or l.lower().startswith("*arcs"):
            if l.lower().startswith("*edge"):
                # switch from multidigraph to multigraph
                G = nx.MultiGraph(G)
            if l.lower().startswith("*arcs"):
                # switch to directed with multiple arcs for each existing edge
                G = G.to_directed()
            for l in lines:
                splitline = l.split()
                if len(splitline) < 2:
                    continue
                ui, vi = splitline[0:2]
                u = nodelabels.get(ui, ui)
                v = nodelabels.get(vi, vi)
                # parse the data attached to this edge and put in a dictionary
                edge_data = {}
                try:
                    # there should always be a single value on the edge?
                    w = splitline[2:3]
                    edge_data.update({'weight': float(w[0])})
                except:
                    pass
                    # if there isn't, just assign a 1
#                    edge_data.update({'value':1})
                extra_attr = zip(splitline[3::2], splitline[4::2])
                edge_data.update(extra_attr)
                # if G.has_edge(u,v):
                #     multigraph=True
                G.add_edge(u, v, **edge_data)
        elif l.lower().startswith("*matrix"):
            G = nx.DiGraph(G)
            adj_list = ((labels[row], labels[col], {'weight': int(data)})
                        for (row, line) in enumerate(lines)
                        for (col, data) in enumerate(line.split())
                        if int(data) != 0)
            G.add_edges_from(adj_list)
    return G

def load_attribute_mapping(pickle_file):
    import pickle
    with open(pickle_file,'rb') as attr_pickle:
        return pickle.load(attr_pickle)

def load_node_set(agent_list):
    with open(agent_list,'r') as agents:
        return set([agent.strip() for agent in agents])

def weighted_core_number(G):
    core_number = {}
    degrees = {}
    if nx.__version__[0] == '1':
        for node, degree in G.degree(weight='weight').items():
            if degree == 0:
                core_number[node] = 0
            else:
                degrees[node] = degree
    elif nx.__version__[0] == '2':
        for node, degree in G.degree(weight='weight'):
            if degree == 0:
                core_number[node] = 0
            else:
                degrees[node] = degree
    while degrees:
        smallest_node = min(degrees, key=degrees.get)
        core_number[smallest_node] = degrees[smallest_node]
        del degrees[smallest_node]
        for node in G.successors(smallest_node):
            if node in degrees: degrees[node] -= G[smallest_node][node]['weight']
        for node in G.predecessors(smallest_node):
            if node in degrees: degrees[node] -= G[node][smallest_node]['weight']
    return core_number

def subgraph_skip(agent_link,subgraph_nodes,half):
    if half == "remgraph":
        return agent_link['enter_ID'] in subgraph_nodes and agent_link['exit_ID'] in subgraph_nodes
    if half == "subgraph":
        return agent_link['enter_ID'] not in subgraph_nodes and agent_link['exit_ID'] not in subgraph_nodes

def save_as_pajek(network_split):
    import traceback
    try:
        # Create a name for this sub-network
        if len(network_split["motifs"]) == 1:
            split_name = network_split["motifs"][0]
        else:
            split_name = set()
            for term in network_split:
                split_name.update(term.split("-"))
            split_name = "".join(split_name)
        if len(network_split["terms"]) == 1:
            split_name += network_split["terms"][0]
        else:

            split_name += "".join((term[0] for term in network_split["terms"])))+network_split["terms"][0][1:]
        if subgraph:
            split_name += network_split["subgraph"]
        print("Running for: ",str(network_split))
        print("Output files will have modifier:",split_name)
        # Load subgraph nodes
        if subgraph:
            print("Reading subgraph node set: "+split_name)
            subgraph_nodes = load_node_set(network_split)
            print("Subgraph is "+str(len(subgraph_nodes))+" nodes")
        else:
            subgraph_nodes = set()
        # Create a network
        G = nx.DiGraph(split=split_name,orig_file=enter_exit_filename)
        # Create a file to put issues
        issues_filename = enter_exit_filename.split('.csv')[0]+'_'+split_name+'.issues'
        # Read in the network
        with open(enter_exit_filename,"r") as enter_exit_file, open(issues_filename,"w") as issues_file:
            print("Reading "+split_name+" from "+enter_exit_filename)
            for agent_link in enter_exit_reader:
                try:
                    if agent_link[motif_term] in network_split["motifs"]:
                        if subgraph and subgraph_skip(agent_link,subgraph_nodes,network_split["subgraph"]): continue
                        weight = sum(float(agent_link[term]) for term in network_split["terms"])
                        if weight > 0:
                            G.add_edge(agent_link['enter_ID'],agent_link['exit_ID'],weight=weight)
                except:
                    issues_file.write("Could not process: "+str(agent_link)+traceback.format_exc())
            # get the total deposits/ed at every node -- this is used to determine receipt of teleporting walkers in InfoMap
            for node in G:
                G.node[node]['out_strength'] = G.out_degree(node,weight='weight')
            # get the core_number of every node -- this is a measure of how near the node is to the center of the network
            print("Getting core number: "+split_name)
            core_numbers = weighted_core_number(G)
            for node in G:
                G.node[node]['core_number'] = core_numbers[node]
            # Loading attributes
            if attr_pickle:
                print("Adding attributes: "+split_name)
                attr_mapping, attr_header = load_attribute_mapping(attr_pickle)
                for node in G:
                    for term in attr_header:
                        try:
                            G.node[node][term] = attr_mapping[node][term]
                        except:
                            G.node[node][term] = ''
                if node_name:
                    # note full unique ID
                    for node in G:
                        G.node[node]['unique_id'] = node
            # save the network as a Pajek file
            pajek_filename = enter_exit_filename.split('.csv')[0]+'_'+split_name+'.net'
            print("Saving Pajek file: "+pajek_filename)
            with open(pajek_filename,'w') as pajek_file:
                for line in generate_pajek(G,teleport='out_strength',name_list=node_name):
                    line += '\n'
                    pajek_file.write(line)
        return pajek_filename
    except:
        print(split_name, traceback.format_exc())

if __name__ == '__main__':
    from shutil import copyfile
    import argparse
    import sys
    import csv
    import os

    ################### ARGUMENTS #####################
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='The input network file (.csv, created by network.py)')
    parser.add_argument('--split_type', action='append', default=[], help='Enter-exit motif to split into a separate sub-network. To join several, use tuples.')
    parser.add_argument('--split_term',action='append', default=['total'], help='Link property to split into a separate sub-network. To join several, use tuples.')
    parser.add_argument('--subgraph', default=None, help='Filename to a set of nodes. Splits the network also into the subgraph of these nodes, and the rest.')
    parser.add_argument('--normalized', action="store_true", default=False, help='Use normalized link weights.')
    parser.add_argument('--attr_pickle', default=None, help='Filename for a pickled dictionary of node attributes. These are added to the network.')
    parser.add_argument('--node_name', default=None, help='Defines a new node name. Attributes separated by commas, or an integer for the first x characters of the node ID.')
    parser.add_argument('--processes', type=int, default=1, help='The max number of parallel processes to launch.')

    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise OSError("Could not find the input file",args.input_file)
    if args.fee_evasion and not os.path.isfile(args.subgraph):
        raise OSError("Could not find the subgraph node list",args.subgraph)
    if args.attr_pickle and not os.path.isfile(args.attr_pickle):
        raise OSError("Could not find the attribute pickle file",args.attr_pickle)
    if args.processes < 1:
        raise ValueError("Please enter a positive number of processes",args.processes)
    if args.node_name and not args.attr_pickle:
        raise ValueError("Node names cannot be changed unless there is an attribute dictionary provided.")
    ####################################################
    network_splits = []
    for motifs in args.split_type:
        motifs = tuple(motif.strip() for motif in motifs.strip('()').split(','))
        for terms in args.split_property
            terms = tuple(term.strip()+"_nrm" for term in terms.strip('()').split(',')) if args.normalized else tuple(term.strip()+"_amt" for term in terms.strip('()').split(','))
            if args.subgraph:
                network_splits.append({"motifs":motifs,"terms":terms,"subgraph":"subgraph"))
                network_splits.append({"motifs":motifs,"terms":terms,"subgraph":"remgraph"))
            else:
                network_splits.append({"motifs":motifs,"terms":terms,"subgraph":None))

    global enter_exit_filename, motif_term, subgraph, attr_pickle, node_name

    enter_exit_filename = args.input_file
    motif_term          = "edge_type_nrm" if args.normalized else "edge_type_amt"
    subgraph            = args.subgraph
    attr_pickle         = args.attr_pickle
    node_name           = [attr for attr in args.node_name.split(",")] if args.node_name else None
    ###################################################
    if args.processes == 1:
        for network_split in network_splits:
            save_as_pajek(network_split)
    else:
        from multiprocessing import Pool
        pool = Pool(processes=args.processes)
        pool.imap(save_as_pajek,network_splits)
        pool.close()
        pool.join()

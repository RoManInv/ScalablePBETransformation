from __future__ import annotations
from collections import defaultdict
import itertools
import time
import traceback

import data.program as prog
import utils.tokens as tk
from utils.timeout import timeout
from utils.key import identifyKeyCand

MAX_ITER = 2

def __df_contain_str__(df, val):
    cols = df.columns
    for col in cols:
        testdf = df[df[col].astype(str).str.contains(val)]
        if(not testdf.empty):
            idxList = testdf.index
            returndf = df.iloc[idxList]
            return returndf
    return None

def dedup_generate(edges, atoms, verbose = False):
    removelist = list()
    for i, (e1, a1) in enumerate(zip(edges, atoms)):
        if(i in removelist):
            continue
        for j, (e2, a2) in enumerate(zip(edges, atoms)):
            if(i >= j):
                continue
            if(e1 == e2 and a1 == a2):
                if(j not in removelist):
                    removelist.append(j)
    for i in removelist[::-1]:
        del(edges[i])
        del(atoms[i])

    return edges, atoms

def split_edges_atoms(edges, atoms):
    edges_split = list()
    atoms_split = list()
    for edge, atom in zip(edges, atoms):
        if(len(set(edge)) == 1):
            edges_split.append(edge)
            atoms_split.append(atom)
        else:
            edge_dict = dict()
            for e, a in zip(edge, atom):
                if(e not in edge_dict.keys()):
                    edge_dict[e] = list()
                edge_dict[e].append(a)
            for newe, newa in edge_dict.items():
                edgelist = list()
                atomlist = list()
                for singleatom in newa:
                    edgelist.append(newe)
                    atomlist.append(singleatom)
                edges_split.append(edgelist)
                atoms_split.append(atomlist)

    return edges_split, atoms_split

def validate(Xis, Ws, node_t, verbose = False):
    if(verbose):
        with open('group_validate.txt', 'a') as f:
            f.write('Target: ' + str(node_t) + '\n')
            f.write('Before validation:\n')
            f.write(str(Xis) + '\n')
            f.write(str(Ws) + '\n')
    Xis_new = list()
    Ws_new = list()
    if(not Xis and not Ws):
        return None, None
    for xi, w in zip(Xis, Ws):
        if(xi and w):
            findcomplete = True
            curr = -1
            removelist = list()
            for i, (edge, atom) in enumerate(zip(xi, w)):
                if(findcomplete):
                    if(edge[1] == curr):
                        removelist.append(i)
                    else:
                        curr = edge[1]
                        findcomplete = False
                if(curr == edge[1]):
                    val = atom.get_value()
                    if(len(node_t) == 1):
                        repidx = 0
                    else:
                        repidx = curr
                    if(val == node_t[repidx]):
                        findcomplete = True
            xi_list = None
            w_list = None
            if(removelist):
                if(xi and w):
                    xi_list = [item for item in xi]
                    w_list = [item for item in w]
                    if(xi_list and w_list):
                        for i in removelist[::-1]:
                            del(xi_list[i])
                            del(w_list[i])
            if(xi_list and w_list):
                Xis_new.append(tuple(xi_list))
                Ws_new.append(tuple(w_list))
    if(verbose):
        with open('group_validate.txt', 'a') as f:
            f.write('After validation:\n')
            f.write(str(Xis_new) + '\n')
            f.write(str(Ws_new) + '\n')
            f.write('==============\n')

    return Xis_new, Ws_new

def validate_single(Xis, Ws, node_t,verbose = False):
    Xis_new = list()
    Ws_new = list()
    # print(node_t)
    if (not len(node_t) == 1):
        return None, None
    else:
        target = node_t[0]
    if(not Xis and not Ws):
        return None, None
    for xi, w in zip(Xis, Ws):
        if(verbose):
            with open('group_validate.txt', 'a') as f:
                f.write('Target: ' + str(node_t) + '\n')
                f.write('Before validation:\n')
                f.write(str(xi) + '\n')
                f.write(str(w) + '\n')
        findcomplete = False
        removelist = list()
        if(len(w) > 1 and w[-1].id == 'ConstStr' and not w[0].id == 'ConstStr'):
            with open('group_validate.txt', 'a') as f:
                f.write('Unnecessary found\n')
            findcomplete = True
        for i, (edge, atom) in enumerate(zip(xi, w)):
            if(str(atom.get_value()) == str(target) and not findcomplete):
                findcomplete = True
                continue
            if(findcomplete):
                removelist.append(i)
        xi_list = None
        w_list = None
        if(removelist):
            if(xi and w):
                xi_list = [item for item in xi]
                w_list = [item for item in w]
                if(xi_list and w_list):
                    for i in removelist[::-1]:
                        if(verbose):
                            with open('group_validate.txt', 'a') as f:
                                f.write('Remove ' + str(i) + '\n')
                        del(xi_list[i])
                        del(w_list[i])
            if(xi_list and w_list):
                Xis_new.append(tuple(xi_list))
                Ws_new.append(tuple(w_list))
        else:
            xi_list = [item for item in xi if item]
            w_list = [item for item in w if item]
            Xis_new.append(tuple(xi_list))
            Ws_new.append(tuple(w_list))

    return Xis_new, Ws_new

class Node:
    def __init__(self, name = None, content = None, nextEdge: Edge = None, isStart: bool = None, isDest: bool = None) -> None:
        self.name = name
        self.content = content
        self.isStart = isStart
        self.isDest = isDest

    def verify(self):
        if(self.isDest):
            self.nextEdge = None



class Edge:
    def __init__(self, name: str, program: prog.Program, nextNode: Node) -> None:
        self.name = name 
        self.program = program

def Make_all_combination(edges, atoms):
    xis = list(itertools.product(*edges))
    Ws = list(itertools.product(*atoms))
    return xis, Ws

def Make_edge_atom_for_each_eta_t(_input, eta_s, eta_t, reversedQS, noTableFlag = False):
    edges_for_each_eta_t = list()
    atoms_for_each_eta_t = list()
    last_idx = len(eta_s) - 1
    orig_eta_s = [str(i) for i in eta_s]
    lookupList_all = set()
    eta_s_old = set([str(item) for item in eta_s])
    
    

    
    for i, node_t in enumerate(eta_t):
        numIter = 0
        iCounter = 0
        eta_s_old = set(orig_eta_s)
        nest_edges, nest_atoms = list(), list()
        counters = {"NumTok": 0, "AlphaTok": 0, "OtherTok": 0}
        
        iterFlag = True
        eta_s = orig_eta_s
        lookupList_all = set()
        while iterFlag:
            numIter += 1
            if(numIter > MAX_ITER):
                return edges_for_each_eta_t, atoms_for_each_eta_t
            if(lookupList_all):
                eta_s = lookupList_all
            for j, node_s in enumerate(eta_s):
                visited = set()
                if(str(node_s) not in visited):
                    visited.add(str(node_s))
                if(not str(node_s) in eta_s_old):
                    eta_s_old.add(node_s)
                try:
                    # atoms, counters, lookupList = atom_search(_input, node_s, node_t, counters, j, last_idx, reversedQS)
                    atoms, counters, lookupList = atom_search_multirow(_input, node_s, node_t, counters, j, last_idx, reversedQS, orig_eta_s, noTableFlag = noTableFlag)
                    if(lookupList):
                        for item in lookupList:
                            if(str(item) not in eta_s_old and str(item) not in lookupList_all):
                                lookupList_all.add(str(item))
                    
                except Exception as e:
                    traceback.print_exc()
                    atoms = -1
                if atoms != -1:
                    for k in range(0, len(atoms)):
                        nest_edges.append((iCounter, i))
                    nest_atoms.extend(atoms)
                    atoms = None
                    edges_for_each_eta_t.append(nest_edges)
                    atoms_for_each_eta_t.append(nest_atoms)
            if(lookupList_all):
                for item in lookupList_all:
                    if item not in eta_s_old:
                        eta_s_old.add(item)
                eta_s = lookupList_all
                lookupList_all = set()
            else:
                lookupList_all = set()
                iterFlag = False
            iCounter += 1
            
            

    return edges_for_each_eta_t, atoms_for_each_eta_t

def Make_edge_atom_for_each_eta_t_group(_input, eta_s, eta_t, groupTableSet, tableColMap, reversedQS):
    edges_for_each_eta_t = list()
    atoms_for_each_eta_t = list()
    last_idx = len(eta_s) - 1
    orig_eta_s = [str(i) for i in eta_s]
    lookupList_all = set()
    eta_s_old = set([str(item) for item in eta_s])

    for i, node_t in enumerate(eta_t):
        numIter = 0
        iCounter = 0
        eta_s_old = set(orig_eta_s)
        nest_edges, nest_atoms = list(), list()
        counters = {"NumTok": 0, "AlphaTok": 0, "OtherTok": 0}
        
        iterFlag = True
        eta_s = orig_eta_s
        lookupList_all = set()
        while iterFlag:
            numIter += 1
            if(numIter > MAX_ITER):
                return edges_for_each_eta_t, atoms_for_each_eta_t
            if(lookupList_all):
                eta_s = lookupList_all
            for j, node_s in enumerate(eta_s):
                visited = set()
                if(str(node_s) not in visited):
                    visited.add(str(node_s))
                if(not str(node_s) in eta_s_old):
                    eta_s_old.add(node_s)
                try:
                    # atoms, counters, lookupList = atom_search(_input, node_s, node_t, counters, j, last_idx, reversedQS)
                    atoms, counters, lookupList = atom_search_multirow_group(_input, node_s, node_t, counters, j, last_idx, groupTableSet, tableColMap, reversedQS, orig_eta_s)
                    if(lookupList):
                        for item in lookupList:
                            if(str(item) not in eta_s_old and str(item) not in lookupList_all):
                                lookupList_all.add(str(item))
                    
                except Exception as e:
                    traceback.print_exc()
                    atoms = -1
                if atoms != -1:
                    for k in range(0, len(atoms)):
                        nest_edges.append((iCounter, i))
                    nest_atoms.extend(atoms)
                    atoms = None
                    edges_for_each_eta_t.append(nest_edges)
                    atoms_for_each_eta_t.append(nest_atoms)
            if(lookupList_all):
                for item in lookupList_all:
                    if item not in eta_s_old:
                        eta_s_old.add(item)
                eta_s = lookupList_all
                lookupList_all = set()
            else:
                lookupList_all = set()
                iterFlag = False
            iCounter += 1
            
            

    return edges_for_each_eta_t, atoms_for_each_eta_t

def atom_search(String, node_s, node_t, counters, s_num, last_idx, reversedQS, numiter = 0, specifyAtom = None):
    if(specifyAtom is None or specifyAtom == 0):
        Atomlist = ["SubStr", "ConstStr", "Lookup"]
    elif(specifyAtom == 1):
        Atomlist = ["Lookup"]
    elif(specifyAtom == 2):
        Atomlist = ["SubStr", "ConstStr"]
    Atomlist = ["SubStr", "ConstStr", "Lookup"]
    Atoms = list()
    for atom in Atomlist:
        if atom == "SubStr":
            flag = False
            try:
                isdecimal = node_s.isdecimal()
            except:
                isdecimal = 0
            try:
                _isalpha = tk.isalpha(node_s)
            except:
                _isalpha = 0
            if isdecimal:
                func = prog.SubStr(String, "NumTok", counters["NumTok"], 0)
                counters["NumTok"] += 1
                if func.get_value() == node_t:
                    Atoms.append(func)
            if _isalpha:
                func = prog.SubStr(String, "AlphaTok", counters["AlphaTok"], 0)
                counters["AlphaTok"] += 1
                if func.get_value() == node_t:
                    Atoms.append(func)
            if s_num == 0 and node_s == node_t:
                func = prog.SubStr(String, "StartTok", 0, 0)
                if func.get_value() == node_t:
                    Atoms.append(func)
            if s_num == last_idx and node_s == node_t:
                func = prog.SubStr(String, "EndTok", 0, 0)
                if func.get_value() == node_t:
                    Atoms.append(func)
        elif atom == "ConstStr":
            func = prog.ConstStr(node_t, 0)
            Atoms.append(func)
        elif atom == "Lookup":
            foundLookup = False
            if(numiter > MAX_ITER):
                return -1
            lookupList = set()
            for tid, tableitem in reversedQS.items():
                for rowitem in tableitem['table'].iterrows():
                    row = rowitem[1].values.tolist()
                    row = [str(i) for i in row]
                    if(node_s in row):
                        if(node_t in row):
                            func = prog.Lookup(node_s, tid, row.index(node_t), row)
                            Atoms.append(func)
                            break
                        else:
                            for rowidx, cell in enumerate(row):
                                if(str(cell) != node_s and str(cell) != String):
                                    func = prog.Lookup(String, tid, row.index(cell), row)
                                    Atoms.append(func)
                                    if not func.get_value() == node_t:
                                        if(str(func.get_value()) not in lookupList and rowidx in tableitem['key']):
                                            lookupList.add(str(func.get_value()))
                                
        else:
            print("atom_search did not match with any constructor.")
            return -1
    return Atoms, counters, lookupList

def atom_search_multirow(String, node_s, node_t, counters, s_num, last_idx, reversedQS, remaining_eta_s, numiter = 0, noTableFlag = False):
    node_s_visited = {i: False for i in [node_s] + list(remaining_eta_s)}
    # if(specifyAtom is None or specifyAtom == 0):
    #     Atomlist = ["SubStr", "ConstStr", "Lookup"]
    # elif(specifyAtom == 1):
    #     Atomlist = ["Lookup"]
    # elif(specifyAtom == 2):
    #     Atomlist = ["SubStr", "ConstStr"]
    if(noTableFlag):
        Atomlist = ["SubStr", "ConstStr"]
    else:
        Atomlist = ["SubStr", "ConstStr", "Lookup"]
    Atoms = list()
    for atom in Atomlist:
        if atom == "SubStr":
            # flag = False
            if(node_s in remaining_eta_s):
                src = [remaining_eta_s.index(node_s)]
            else:
                src = [-1]
            try:
                isdecimal = node_s.isdecimal()
            except:
                isdecimal = 0
            try:
                _isalpha = tk.isalpha(node_t)
            except:
                _isalpha = 0
            # if isdecimal:
            #     func = prog.SubStr(node_s, "NumTok", counters["NumTok"], 0, src)
            #     counters["NumTok"] += 1
            #     if func.get_value() == node_t:
            #         Atoms.append(func)
            # if _isalpha:
            #     func = prog.SubStr(node_s, "AlphaTok", counters["AlphaTok"], 0, src)
            #     counters["AlphaTok"] += 1
            #     if func.get_value() == node_t:
            #         Atoms.append(func)
            # if s_num == 0 and node_s == node_t:
            #     func = prog.SubStr(node_s, "StartTok", 0, 0, src)
            #     if func.get_value() == node_t:
            #         Atoms.append(func)
            # if s_num == last_idx and node_s == node_t:
            #     func = prog.SubStr(node_s, "EndTok", 0, 0, src)
            #     if func.get_value() == node_t:
            #         Atoms.append(func)
            node_s_list = node_s.split()
            if(node_t in node_s_list):
                if(isdecimal):
                    func = prog.SubStr(node_s, "NumTok", counters["NumTok"], node_s_list.index(node_t), src)
                    Atoms.append(func)
                elif(_isalpha):
                    func = prog.SubStr(node_s, "AlphaTok", counters["NumTok"], node_s_list.index(node_t), src)
                    Atoms.append(func)
        elif atom == "ConstStr":
            func = prog.ConstStr(node_t, 0)
            Atoms.append(func)
        elif atom == "Lookup":
            foundLookup = False
            if(numiter > MAX_ITER):
                return -1
            lookupList = set()
            for tid, tableitem in reversedQS.items():
                table_max_row_cover = 1
                tab = tableitem['table']
                for rowitem in tableitem['table'].iterrows():
                    row = rowitem[1].values.tolist()
                    row = [str(i) for i in row]
                    coverList = list()
                    coverIdxList = list()
                    if(node_s in row):
                        table_curr_row_cover = 1
                        node_s_visited[node_s] = True
                        coverList.append(node_s)
                        coverIdxList.append(row.index(node_s))
                        for token in remaining_eta_s:
                            if(str(token) != str(node_s) and str(token) in row):
                                coverList.append(token)
                                coverIdxList.append(row.index(node_s))
                                node_s_visited[token] = True
                                table_curr_row_cover += 1
                                if(table_curr_row_cover > table_max_row_cover):
                                    table_max_row_cover = table_curr_row_cover
                        if(table_curr_row_cover < table_max_row_cover):
                            continue
                        if(not len(tab.iloc[:, coverIdxList].drop_duplicates()) == len(tab)):
                            continue
                        if(node_t in row):
                            src = list()
                            for itemcover in coverList:
                                if(itemcover in remaining_eta_s):
                                    src.append(remaining_eta_s.index(itemcover))
                                else:
                                    src.append(-1)
                            func = prog.Lookup(coverList, tid, row.index(node_t), row, src)
                            Atoms.append(func)
                            break
                        else:
                            for rowidx, cell in enumerate(row):
                                if(cell != node_s and cell not in coverList and str(cell) not in remaining_eta_s and cell != String):
                                    src = list()
                                    for itemcover in coverList:
                                        if(itemcover in remaining_eta_s):
                                            src.append(remaining_eta_s.index(itemcover))
                                        else:
                                            src.append(-1)
                                    func = prog.Lookup(coverList, tid, row.index(cell), row, src)
                                    Atoms.append(func)
                                    if(not str(func.get_value()) == node_t):
                                        if(str(func.get_value()) not in lookupList and rowidx in tableitem['key']):
                                            lookupList.add(str(func.get_value()))

            for key, val in node_s_visited.items():
                if(not val):
                    lookupList.add(str(key))



        else:
            print("atom_search did not match with any constructor.")
            return -1
    return Atoms, counters, lookupList

def atom_search_multirow_group(String, node_s, node_t, counters, s_num, last_idx, groupTableSet, tableColMap, reversedQS, remaining_eta_s, numiter = 0, specifyAtom = None):
    node_s_visited = {i: False for i in [node_s] + list(remaining_eta_s)}
    tableVisited = set()
    if(specifyAtom is None or specifyAtom == 0):
        Atomlist = ["SubStr", "ConstStr", "Lookup"]
    elif(specifyAtom == 1):
        Atomlist = ["Lookup"]
    elif(specifyAtom == 2):
        Atomlist = ["SubStr", "ConstStr"]
    Atomlist = ["SubStr", "ConstStr", "Lookup"]
    Atoms = list()
    for atom in Atomlist:
        if atom == "SubStr":
            flag = False
            if(node_s in remaining_eta_s):
                src = [remaining_eta_s.index(node_s)]
            else:
                src = [-1]
            try:
                isdecimal = node_s.isdecimal()
            except:
                isdecimal = 0
            try:
                _isalpha = tk.isalpha(node_s)
            except:
                _isalpha = 0
            if isdecimal:
                func = prog.SubStr(node_s, "NumTok", counters["NumTok"], 0, src)
                counters["NumTok"] += 1
                if func.get_value() == node_t:
                    Atoms.append(func)
            if _isalpha:
                func = prog.SubStr(node_s, "AlphaTok", counters["AlphaTok"], 0, src)
                counters["AlphaTok"] += 1
                if func.get_value() == node_t:
                    Atoms.append(func)
            if s_num == 0 and node_s == node_t:
                func = prog.SubStr(node_s, "StartTok", 0, 0, src)
                if func.get_value() == node_t:
                    Atoms.append(func)
            if s_num == last_idx and node_s == node_t:
                func = prog.SubStr(node_s, "EndTok", 0, 0, src)
                if func.get_value() == node_t:
                    Atoms.append(func)
        elif atom == "ConstStr":
            func = prog.ConstStr(node_t, 0)
            Atoms.append(func)
        elif atom == "Lookup":
            foundLookup = False
            if(numiter > MAX_ITER):
                return -1
            lookupList = set()
            for tid, tableitem in reversedQS.items():
                if(tid in tableVisited):
                    continue
                else:
                    tableVisited.add(tid)
                table_max_row_cover = 1
                tab = tableitem['table']
                for rowitem in tableitem['table'].iterrows():
                    row = rowitem[1].values.tolist()
                    row = [str(i) for i in row]
                    coverList = list()
                    coverIdxList = list()
                    if(node_s in row):
                        table_curr_row_cover = 1
                        node_s_visited[node_s] = True
                        coverList.append(node_s)
                        coverIdxList.append(row.index(node_s))
                        for token in remaining_eta_s:
                            if(str(token) != str(node_s) and str(token) in row):
                                coverList.append(token)
                                coverIdxList.append(row.index(node_s))
                                node_s_visited[token] = True
                                table_curr_row_cover += 1
                                if(table_curr_row_cover > table_max_row_cover):
                                    table_max_row_cover = table_curr_row_cover
                        if(table_curr_row_cover < table_max_row_cover):
                            continue
                        if(not len(tab.iloc[:, coverIdxList].drop_duplicates()) == len(tab)):
                            continue
                        if(node_t in row):
                            src = list()
                            for itemcover in coverList:
                                if(itemcover in remaining_eta_s):
                                    src.append(remaining_eta_s.index(itemcover))
                                else:
                                    src.append(-1)
                            func = prog.Lookup(coverList, tid, row.index(node_t), row, src)
                            Atoms.append(func)
                            for tableset in groupTableSet:
                                if(tid in tableset):
                                    for grouptid in tableset:
                                        if(grouptid not in tableVisited and grouptid in reversedQS.keys()):
                                            currGroupTable = reversedQS[grouptid]['table']
                                            for currGroupCol in currGroupTable.columns:
                                                foundRow = currGroupTable[currGroupTable[currGroupCol].str.contains(node_t, na = False)]
                                                # foundRow = foundRow.fillna(-1)
                                                if(not foundRow.empty):
                                                    foundFlag = False
                                                    foundRow = foundRow.values.tolist()
                                                    for i, currFoundRow in enumerate(foundRow):
                                                        if(node_s in currFoundRow):
                                                            foundRow = foundRow[i]
                                                            foundFlag = True
                                                            break
                                                    if(not foundFlag):
                                                        foundRow = None
                                                else:
                                                    foundRow = None
                                            if(foundRow):
                                                # currGroupSrc = []
                                                func = prog.Lookup(coverList, grouptid, foundRow.index(node_t), foundRow, src)
                                                Atoms.append(func)
                                                tableVisited.add(grouptid)
                            break
                        else:
                            for rowidx, cell in enumerate(row):
                                if(cell != node_s and cell not in coverList and str(cell) not in remaining_eta_s and cell != String):
                                    src = list()
                                    for itemcover in coverList:
                                        if(itemcover in remaining_eta_s):
                                            src.append(remaining_eta_s.index(itemcover))
                                        else:
                                            src.append(-1)
                                    func = prog.Lookup(coverList, tid, row.index(cell), row, src)
                                    Atoms.append(func)
                                    if(not str(func.get_value()) == node_t):
                                        if(str(func.get_value()) not in lookupList and rowidx in tableitem['key']):
                                            lookupList.add(str(func.get_value()))

            for key, val in node_s_visited.items():
                if(not val):
                    lookupList.add(str(key))



        else:
            print("atom_search did not match with any constructor.")
            return -1
    return Atoms, counters, lookupList

class Graph(object):
    """ Graph data structure, undirected by default. """

    def __init__(self, connections, directed = True):
        self._graph = defaultdict(set)
        self._directed = directed
        self.add_connections(connections)

    def add_connections(self, connections):
        """ Add connections (list of tuple pairs) to graph """

        for node1, node2, edge in connections:
            self.add(node1, node2, edge)

    def add(self, node1, node2, edge):
        """ Add connection between node1 and node2 """

        self._graph[node1].add((node2, edge))
        if not self._directed:
            self._graph[node2].add((node1, edge))

    def remove(self, node):
        """ Remove all references to node """

        for n, cxns in self._graph.items():  # python3: items(); python2: iteritems()
            try:
                cxns.remove(node)
            except KeyError:
                pass
        try:
            del self._graph[node]
        except KeyError:
            pass

    def is_connected(self, node1, node2):
        """ Is node1 directly connected to node2 """

        return (node1 in self._graph) and (node2 in [nodelist[0]] for nodelist in self._graph[node1])

    def find_path(self, node1, node2, path=[]):
        """ Find any path between node1 and node2 (may not be shortest) """

        path = path + [node1]
        if node1 == node2:
            return path
        if node1 not in self._graph:
            return None
        for node in ([nodelist[0]] for nodelist in self._graph[node1]):
            if node not in path:
                new_path = self.find_path(node, node2, path)
                if new_path:
                    return new_path
        return None

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, dict(self._graph))

    def __repr__(self):
        self.__str__()

def validate_output_atom(prog, inputval):
    if(prog.id == 'SubStr'):
        if(inputval == prog.String):
            return True
        else:
            return False
    elif(prog.id == 'ConstStr'):
        return True
    elif(prog.id == 'Lookup'):
        if(inputval in prog.row):
            return True
        else:
            return False

def validate_atoms(xis, Ws, eta_s, eta_t, verbose = False):
    xis_new = list()
    Ws_new = list()
    if(len(eta_t) == 1):
        singletest = True
    else:
        singletest = False
    for xi, W in zip(xis, Ws):
        progs_new = list()
        progDict = dict()
        for edge, atom in zip(xi, W):
            i = edge[1]
            j = edge[0]
            
            if(i not in progDict.keys()):
                progDict[i] = list()
            progDict[i].append(atom)
        progDict_new = dict()
        if(verbose):
            with open('detail.txt', 'a') as f:
                f.write(str(progDict))
                f.write('\n')
                f.write(str(eta_t) + '\n')
                for idx, progs in progDict.items():
                    if(singletest):
                        residx = 0
                    else:
                        residx = idx
                    f.write(str(eta_t[residx]) + '\n')
                    j = 0
                    inputval = eta_s[0]
                    found = False
                    for prog in progs:
                        f.write(str(prog) + '\n')
                        j += 1
                        if(prog.get_value() == eta_t[residx]):
                            found = True
                            break
                    if(found):
                        progs_new = progs[:j]
                    else:
                        progs_new = list()
                    progDict_new[idx] = progs_new
        else:
            for idx, progs in progDict.items():
                if(singletest):
                    residx = 0
                else:
                    residx = idx
                j = 0
                inputval = eta_s[0]
                found = False
                for prog in progs:
                    j += 1
                    if(prog.get_value() == eta_t[residx]):
                        found = True
                        break
                if(found):
                    progs_new = progs[:j]
                else:
                    progs_new = list()
                progDict_new[idx] = progs_new
        j = 0
        xi_new = list()
        W_new = list()
        for edgei, atoms in progDict_new.items():
            for atom in atoms:
                xi_new.append((j, edgei))
                W_new.append(atom)
                j += 1
        if(xi_new and W_new):
            xis_new.append(tuple(xi_new))
            Ws_new.append(tuple(W_new))
    
    return xis_new, Ws_new

def dedup_validated(xis, Ws):
    eaDict = dict()

    for edges, atoms in zip(xis, Ws):
        for xi, w in zip(edges, atoms):
            if(xi not in eaDict.keys()):
                eaDict[xi] = set()
            if(w not in eaDict[xi]):
                eaDict[xi].add(w)

    xis_new = list()
    Ws_new = list()

    for xi, atoms in eaDict.items():
        for atom in atoms:
            xis_new.append(xi)
            Ws_new.append(atom)

    return xis_new, Ws_new

def remove_unnecessary(xis, Ws, eta_t):
    xis_new = list()
    Ws_new = list()
    for xi, w in zip(xis, Ws):
        removelist = list()
        while (len(w) > 1 and w[-1].id == 'ConstStr'):
            xi = list(xi)
            del(xi[-1])
            xi = tuple(xi)
            w = list(w)
            del(w[-1])
            w = tuple(w)

        if(w not in Ws_new and w[-1].get_value() == eta_t):
            xis_new.append(xi)
            Ws_new.append(w)
    return xis_new, Ws_new

# fix this!
# def remove_fewer_cover(xis, Ws):
#     covernumdict = dict()
#     currcover = -1
#     for i, (xi, w) in enumerate(zip(xis, Ws)):
#         for edges, atoms in zip(xi, w):
#             traversedlist = list()
#             removelist = list()
#             for e, a in zip(edges, atoms):
#                 lookupflag = False
#                 if(e[1] not in covernumdict.keys()):
#                     covernumdict[e[1]] = currcover
#                 if(a.id == 'Lookup'):
#                     lookupflag = True
#                     if(type (a.String) is list):
#                         if(len(a.String) > currcover):
#                             currcover = len(a.String)
#                     else:
#                         currcover = 1
#             if(lookupflag):
#                 traversedlist.append(i)
#                 if(currcover > covernumdict[e[1]]):
#                     covernumdict[e[1]] = currcover
#                     removelist.extend(traversedlist)
#                     traversedlist = list()
#                 elif(currcover < covernumdict[e[1]]):
#                     if i not in removelist:
#                         removelist.append(i)
            
#     removelist = sorted(removelist)
#     for i in removelist[::-1]:
#         del(xis[i])
#         del(Ws[i])

#     return xis, Ws

def remove_fewer_cover(xis, Ws):
    covernumdict = dict()
    for i, (xi, w) in enumerate(zip(xis, Ws)):
        if(i not in covernumdict.keys()):
            covernumdict[i] = -1
        for edges, atoms in zip(xi, w):
            currcover = -1
            for edge, atom in zip(edges, atoms):
                if(atom.id == 'Lookup'):
                    currcover = len(atom.String)
            if(currcover > covernumdict[i]):
                covernumdict[i] = currcover
    
    removelistlist = list()
    for i, (xi, w) in enumerate(zip(xis, Ws)):
        removelistlist.append(list())
        for j, (edges, atoms) in enumerate(zip(xi, w)):
            currcover = -1
            for edge, atom in zip(edges, atoms):
                if(atom.id == 'Lookup'):
                    currcover = len(atom.String)
            if(currcover < covernumdict[i]):
                removelistlist[i].append(j)
    
    for i, removelist in enumerate(removelistlist):
        for j in removelist[::-1]:
            xis = list(xis)
            xis = [list(i) for i in xis]
            del(xis[i][j])
            Ws = list(Ws)
            Ws = [list(i) for i in Ws]
            del(Ws[i][j])
    
    return xis, Ws

def lookupValidate(xis, Ws):
    removelist = list()
    for i, (edges, atoms) in enumerate(zip(xis, Ws)):
        curranswer = None
        for e, a in zip(edges, atoms):
            if not curranswer:
                curranswer = a.get_value()
            else:
                if(a.id == 'Lookup' and curranswer not in a.row):
                    if(i not in removelist):
                        removelist.append(i)
                        break
                curranswer = a.get_value()
    removelist = sorted(removelist)
    for i in removelist[::-1]:
        del(xis[i])
        del(Ws[i])
    return xis, Ws

def group_up(xis, Ws):
    xis_dict = dict()
    Ws_dict = dict()

    for xi, w in zip(xis, Ws):
        tokidx = xi[0][1]
        if(tokidx not in xis_dict.keys()):
            xis_dict[tokidx] = list()
        if(tokidx not in Ws_dict.keys()):
            Ws_dict[tokidx] = list()

        xis_dict[tokidx].append(xi)
        Ws_dict[tokidx].append(w)

    xis_grouped = list()
    ws_grouped = list()

    for key in xis_dict.keys():
        xis_grouped.append(tuple(xis_dict[key]))
        ws_grouped.append(tuple(Ws_dict[key]))

    return xis_grouped, ws_grouped

def GENERATE(_input, _output, reversedQS, noTableFlag = False, verbose = False):
    groupDict = dict()
    if(type(_input) is list or type(_input) is tuple):
        eta_s = list()
        for item in _input:
            # eta_s_temp = prog.Makenode(str(item), [])
            # eta_s.extend(eta_s_temp)
            eta_s.append(str(item))
        for item in _input:
            eta_s_temp = prog.Makenode(str(item), [])
            # eta_s_temp = prog.Makenode_comb(str(item), [])
            eta_s.extend(eta_s_temp)
        print(eta_s)
    # eta_s = prog.Makenode(str(_input), [])
    # eta_t = prog.Makenode(str(_output), [])
    eta_t = [_output]
    print(eta_t)
    eta = [eta_s, eta_t]
    try:
        if(verbose):
            print("making edge for " + str(_input) + " AND " + str(_output))
        currtime = time.time()
        edges, atoms = Make_edge_atom_for_each_eta_t(_input, eta_s, eta_t, reversedQS, noTableFlag = noTableFlag)
        print("\t\tTime for raw graph generation: " + str(time.time() - currtime))
        if(verbose):
            with open('edges.txt', 'w') as f:
                for edge, atom in zip(edges, atoms):
                    f.write('edge: ' + str(edge) + '\n')
                    f.write('atom: ' + str(atom) + '\n')
                    f.write('==========\n')
            print('Number edges: ' + str(len(edges)))
            print('Number atoms: ' + str(len(atoms)))
        edges, atoms = dedup_generate(edges, atoms, verbose)
        edges, atoms = split_edges_atoms(edges, atoms)
        print(len(atoms))
        # if(len(atoms) > 3000):
        #     print("This file is temporarily skipped due to taking too long")
        #     return None
        
        for xis, ws in zip(edges, atoms):
            for xi, w in zip(xis, ws):
                if(xi[1] not in groupDict.keys()):
                    groupDict[xi[1]] = dict()
                if(xi[0] not in groupDict[xi[1]].keys()):
                    groupDict[xi[1]][xi[0]] = list()
                groupDict[xi[1]][xi[0]].append(w)
        if(verbose):
            with open('groupdict.txt', 'w') as f:
                for key, val in groupDict.items():
                    f.write(str(key) + '\n')
                    for k, v in val.items():
                        f.write(str(k) + ':' + str(v) + '\n')
                    f.write('-----------------\n')
                f.write('=====================\n')
                f.write(str(groupDict))
        xis_grouped, Ws_grouped = list(), list()
        if(verbose):
            with open('group_validate.txt', 'w') as f:
                f.write('Group validation\n')
            with open('detail.txt', 'w') as f:
                f.write('Validating atom programs \n')
        for j in groupDict.keys():
            Ws_g = list()
            xis_g = list()
            for i, progs in groupDict[j].items():
                Ws_g.append(progs)
                xis_g.append([(i, j)] * len(progs))
            if(verbose):
                with open('groupdetail.txt', 'a') as f:
                    f.write(str(xis_g) + '\n')
                    f.write(str(Ws_g) + '\n')
            currtime = time.time()
            xis_g, Ws_g = Make_all_combination(xis_g, Ws_g)
            print("\t\tTime for generating graph connections: " + str(time.time() - currtime))
            
            currtime = time.time()
            xis_g, Ws_g = validate_single(xis_g, list(Ws_g), [eta_t[j]], verbose)
            xis_g, Ws_g = validate_atoms(xis_g, Ws_g, eta_s, [eta_t[j]], verbose)
            xis_g, Ws_g = remove_unnecessary(xis_g, Ws_g, eta_t[j])
            
            if(verbose):
                with open('groupcomb.txt', 'a') as f:
                    for xi, w in zip(xis_g, Ws_g):
                        f.write(str(xi) + ': ' + str(w) + '\n')
            xis_g, Ws_g = lookupValidate(xis_g, Ws_g)
            print("\t\tTime for generating graph validation: " + str(time.time() - currtime))
            
            
            
            
            # here validate each program
            if(xis_g and Ws_g):
                xis_grouped.append(xis_g)
                Ws_grouped.append(Ws_g)
        # here create final progs
        
    except Exception as e:
        print('combination not found')
        traceback.print_exc()
        xis = None
        Ws = None
    finally:
        print("Combinations search complete")
        pass

    if(verbose):
        with open('detail_validated.txt', 'a') as f:
            for xi, W in zip(xis_grouped, Ws_grouped):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')

    xis_grouped = [tuple(i) for i in xis_grouped]
    Ws_grouped = [tuple(i) for i in Ws_grouped]
    currtime = time.time()
    xis, Ws = dedup_validated(xis_grouped, Ws_grouped)
    xis, Ws = group_up(xis, Ws)
    
    if(verbose):
        with open('detail_validated_dedup.txt', 'a') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    xis, Ws = remove_fewer_cover(xis, Ws)
    print("\t\tTime for generating final paths: " + str(time.time() - currtime))
    
    
    currtime = time.time()
    xis, Ws = Make_all_combination(xis, Ws)
    print("\t\tTime for generating final graphs: " + str(time.time() - currtime))
    # xis, Ws = dedup_validated(xis, Ws) #??
    if(verbose):
        with open('final_edges_and_atoms.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    
    
    

    if(xis is None and Ws is None):
        if(verbose):
            print('Combination not found from previous step')
        return None
    data_structures = list()
    if(verbose):
        with open('graph.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write('edge: ' + str(xi) + '\n')
                f.write('atom: ' + str(W) + '\n')
                f.write('--------------\n')
                print(xi, W)
                data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
                data_structures.append(data_structure)
    else:
        for xi, W in zip(xis, Ws):
            data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
            data_structures.append(data_structure)
    return data_structures

def GENERATE_minimal(_input, _output, reversedQS, remaining, verbose = False):
    groupDict = dict()
    remaininglist = set()
    if(type(_input) is list or type(_input) is tuple):
        eta_s = list()
        # for item in _input:
            # eta_s_temp = prog.Makenode(str(item), [])
            # eta_s.extend(eta_s_temp)
            # eta_s.append(str(item))
        # print(eta_s)
    eta_s = prog.Makenode(str(_input), [])
    for inputstr in remaining:
        for item in inputstr:
            remaininglist.add(str(item))
    eta_t = prog.Makenode(str(_output), [])
    eta = [eta_s, eta_t]
    try:
        if(verbose):
            print("making edge for " + str(_input) + " AND " + str(_output))
        edges, atoms = Make_edge_atom_for_each_eta_t(_input, eta_s, eta_t, reversedQS)
        if(verbose):
            with open('edges.txt', 'w') as f:
                for edge, atom in zip(edges, atoms):
                    f.write('edge: ' + str(edge) + '\n')
                    f.write('atom: ' + str(atom) + '\n')
                    f.write('==========\n')
            print('Number edges: ' + str(len(edges)))
            print('Number atoms: ' + str(len(atoms)))
        edges, atoms = dedup_generate(edges, atoms, verbose)
        edges, atoms = split_edges_atoms(edges, atoms)
        print(len(atoms))
        
        for xis, ws in zip(edges, atoms):
            for xi, w in zip(xis, ws):
                if(xi[1] not in groupDict.keys()):
                    groupDict[xi[1]] = dict()
                if(xi[0] not in groupDict[xi[1]].keys()):
                    groupDict[xi[1]][xi[0]] = list()
                groupDict[xi[1]][xi[0]].append(w)
        if(verbose):
            with open('groupdict.txt', 'w') as f:
                for key, val in groupDict.items():
                    f.write(str(key) + '\n')
                    for k, v in val.items():
                        f.write(str(k) + ':' + str(v) + '\n')
                    f.write('-----------------\n')
                f.write('=====================\n')
                f.write(str(groupDict))
        xis_grouped, Ws_grouped = list(), list()
        if(verbose):
            with open('group_validate.txt', 'w') as f:
                f.write('Group validation\n')
            with open('detail.txt', 'w') as f:
                f.write('Validating atom programs \n')
        for j in groupDict.keys():
            Ws_g = list()
            xis_g = list()
            for i, progs in groupDict[j].items():
                Ws_g.append(progs)
                xis_g.append([(i, j)] * len(progs))
            if(verbose):
                with open('groupdetail.txt', 'a') as f:
                    f.write(str(xis_g) + '\n')
                    f.write(str(Ws_g) + '\n')
            xis_g, Ws_g = Make_all_combination(xis_g, Ws_g)
            
            
            xis_g, Ws_g = validate_single(xis_g, list(Ws_g), [eta_t[j]], verbose)
            xis_g, Ws_g = validate_atoms(xis_g, Ws_g, eta_s, [eta_t[j]], verbose)
            xis_g, Ws_g = remove_unnecessary(xis_g, Ws_g, eta_t[j])
            
            if(verbose):
                with open('groupcomb.txt', 'a') as f:
                    for xi, w in zip(xis_g, Ws_g):
                        f.write(str(xi) + ': ' + str(w) + '\n')
            xis_g, Ws_g = lookupValidate(xis_g, Ws_g)
            
            
            
            
            # here validate each program
            if(xis_g and Ws_g):
                xis_grouped.append(xis_g)
                Ws_grouped.append(Ws_g)
        # here create final progs
        
    except Exception as e:
        print('combination not found')
        traceback.print_exc()
        xis = None
        Ws = None
    finally:
        print("Combinations search complete")
        pass

    if(verbose):
        with open('detail_validated.txt', 'a') as f:
            for xi, W in zip(xis_grouped, Ws_grouped):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')

    xis_grouped = [tuple(i) for i in xis_grouped]
    Ws_grouped = [tuple(i) for i in Ws_grouped]
    xis, Ws = dedup_validated(xis_grouped, Ws_grouped)
    xis, Ws = group_up(xis, Ws)
    
    if(verbose):
        with open('detail_validated_dedup.txt', 'a') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    xis, Ws = remove_fewer_cover(xis, Ws)
    
    
    
    xis, Ws = Make_all_combination(xis, Ws)
    # xis, Ws = dedup_validated(xis, Ws) #??
    if(verbose):
        with open('final_edges_and_atoms.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    
    
    

    if(xis is None and Ws is None):
        if(verbose):
            print('Combination not found from previous step')
        return None
    data_structures = list()
    if(verbose):
        with open('graph.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write('edge: ' + str(xi) + '\n')
                f.write('atom: ' + str(W) + '\n')
                f.write('--------------\n')
                print(xi, W)
                data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
                data_structures.append(data_structure)
    else:
        for xi, W in zip(xis, Ws):
            data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
            data_structures.append(data_structure)
    return data_structures

def GENERATE_group(_input, _output, reversedQS, groupTableSet, tableColMap, verbose = False):
    groupDict = dict()
    if(type(_input) is list or type(_input) is tuple):
        eta_s = list()
        for item in _input:
            # eta_s_temp = prog.Makenode(str(item), [])
            # eta_s.extend(eta_s_temp)
            eta_s.append(str(item))
        for item in _input:
            eta_s_temp = prog.Makenode(str(item), [])
            # eta_s_temp = prog.Makenode_comb(str(item), [])
            eta_s.extend(eta_s_temp)
        print(eta_s)
    # eta_s = prog.Makenode(str(_input), [])
    eta_t = prog.Makenode(str(_output), [])
    eta = [eta_s, eta_t]
    try:
        if(verbose):
            print("making edge for " + str(_input) + " AND " + str(_output))
        edges, atoms = Make_edge_atom_for_each_eta_t_group(_input, eta_s, eta_t, groupTableSet, tableColMap, reversedQS)
        if(verbose):
            with open('edges.txt', 'w') as f:
                for edge, atom in zip(edges, atoms):
                    f.write('edge: ' + str(edge) + '\n')
                    f.write('atom: ' + str(atom) + '\n')
                    f.write('==========\n')
            print('Number edges: ' + str(len(edges)))
            print('Number atoms: ' + str(len(atoms)))
        edges, atoms = dedup_generate(edges, atoms, verbose)
        edges, atoms = split_edges_atoms(edges, atoms)
        print(len(atoms))
        if(len(atoms) > 3000):
            print("This file is temporarily skipped due to taking too long")
            return None
        
        for xis, ws in zip(edges, atoms):
            for xi, w in zip(xis, ws):
                if(xi[1] not in groupDict.keys()):
                    groupDict[xi[1]] = dict()
                if(xi[0] not in groupDict[xi[1]].keys()):
                    groupDict[xi[1]][xi[0]] = list()
                groupDict[xi[1]][xi[0]].append(w)
        if(verbose):
            with open('groupdict.txt', 'w') as f:
                for key, val in groupDict.items():
                    f.write(str(key) + '\n')
                    for k, v in val.items():
                        f.write(str(k) + ':' + str(v) + '\n')
                    f.write('-----------------\n')
                f.write('=====================\n')
                f.write(str(groupDict))
        xis_grouped, Ws_grouped = list(), list()
        if(verbose):
            with open('group_validate.txt', 'w') as f:
                f.write('Group validation\n')
            with open('detail.txt', 'w') as f:
                f.write('Validating atom programs \n')
        for j in groupDict.keys():
            Ws_g = list()
            xis_g = list()
            for i, progs in groupDict[j].items():
                Ws_g.append(progs)
                xis_g.append([(i, j)] * len(progs))
            if(verbose):
                with open('groupdetail.txt', 'a') as f:
                    f.write(str(xis_g) + '\n')
                    f.write(str(Ws_g) + '\n')
            xis_g, Ws_g = Make_all_combination(xis_g, Ws_g)
            
            
            xis_g, Ws_g = validate_single(xis_g, list(Ws_g), [eta_t[j]], verbose)
            xis_g, Ws_g = validate_atoms(xis_g, Ws_g, eta_s, [eta_t[j]], verbose)
            xis_g, Ws_g = remove_unnecessary(xis_g, Ws_g, eta_t[j])
            
            if(verbose):
                with open('groupcomb.txt', 'a') as f:
                    for xi, w in zip(xis_g, Ws_g):
                        f.write(str(xi) + ': ' + str(w) + '\n')
            xis_g, Ws_g = lookupValidate(xis_g, Ws_g)
            
            
            
            
            # here validate each program
            if(xis_g and Ws_g):
                xis_grouped.append(xis_g)
                Ws_grouped.append(Ws_g)
        # here create final progs
        
    except Exception as e:
        print('combination not found')
        traceback.print_exc()
        xis = None
        Ws = None
    finally:
        print("Combinations search complete")
        pass

    if(verbose):
        with open('detail_validated.txt', 'a') as f:
            for xi, W in zip(xis_grouped, Ws_grouped):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')

    xis_grouped = [tuple(i) for i in xis_grouped]
    Ws_grouped = [tuple(i) for i in Ws_grouped]
    xis, Ws = dedup_validated(xis_grouped, Ws_grouped)
    xis, Ws = group_up(xis, Ws)
    
    if(verbose):
        with open('detail_validated_dedup.txt', 'a') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    xis, Ws = remove_fewer_cover(xis, Ws)
    
    
    
    xis, Ws = Make_all_combination(xis, Ws)
    # xis, Ws = dedup_validated(xis, Ws) #??
    if(verbose):
        with open('final_edges_and_atoms.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write(str(xi) + '\n')
                f.write(str(W) + '\n')
                f.write('===========\n')
    
    
    

    if(xis is None and Ws is None):
        if(verbose):
            print('Combination not found from previous step')
        return None
    data_structures = list()
    if(verbose):
        with open('graph.txt', 'w') as f:
            for xi, W in zip(xis, Ws):
                f.write('edge: ' + str(xi) + '\n')
                f.write('atom: ' + str(W) + '\n')
                f.write('--------------\n')
                print(xi, W)
                data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
                data_structures.append(data_structure)
    else:
        for xi, W in zip(xis, Ws):
            data_structure = DAG(eta, eta_s, eta_t, xi, W, 0, 0)
            data_structures.append(data_structure)
    return data_structures

def GENERATE_scalable(_input, _output, reversedQS, noTableFlag = False, verbose = False):
    """
    return structure:
    {int: {'graph': graph, 'support': int}}
    """
    input_single = _input[0]
    output_single = _output[0]

    input_remaining = _input[1:]
    output_remaining = _output[1:]

    graphs = GENERATE(input_single, output_single, reversedQS, noTableFlag = False, verbose = verbose)
    if(graphs is None):
        return None

    graphsupport = list()
    for g in graphs:
        support = 1
        tempdict = {'graph': g, 'support': 1, 'support_list': [g.eta_s]}
        suplist = list()
        for x, y in zip(input_remaining, output_remaining):
            ansdict = discover([x], reversedQS, g)
            if(ansdict[tuple(x)] == y):
                support += 1
                suplist.append(x)
        tempdict['support'] = support
        tempdict['support_list'] += suplist
        graphsupport.append(tempdict)

    return graphsupport


def discover(Q, reversedQS, graph):
        ansDict = dict()
        for q in Q:
            transformation = ''
            for xis, Ws in zip(graph.xi, graph.W):
                maxlen = max(xis, key = lambda x: x[0])[0]
                outputParts = [None] * (maxlen + 1)
                FirstProg = True
                # currval = ''
                for xi, atom in zip(xis, Ws):
                    if(atom.id == 'ConstStr'):
                        outputParts[xi[0]] = atom.get_value()
                    elif(atom.id == 'SubStr'):
                        if(FirstProg):
                            atom.String = q
                            outputParts[xi[0]] = atom.get_value()
                            FirstProg = False
                        else:
                            atom.String = outputParts[xi[0]]
                            outputParts[xi[0]] = atom.get_value()
                    elif(atom.id == 'Lookup'):
                        if(FirstProg):
                            try:
                                atom.String = (q[atom.src[0]],)
                            except:
                                print(atom.src)
                            atom.row = __get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
                            outputParts[xi[0]] = atom.get_value()
                            FirstProg = False
                        else:
                            atom.String = outputParts[xi[0]]
                            atom.String = list()
                            for val in atom.src:
                                if(val == -1):
                                    atom.String.append(outputParts[xi[0]])
                                else:
                                    atom.String.append(q[val])
                            atom.String = tuple(atom.String)
                            atom.row = __get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
                            maxlen[xi[0]] = atom.get_value()
                # transformation += currval
                transformation = ''.join([i for i in outputParts if i is not None])
            if(tuple(q) not in ansDict.keys()):
                ansDict[tuple(q)] = ''
            ansDict[tuple(q)] = transformation
        return ansDict

def __get_row_from_table__(table, col, val):
        if((type(col) is int or type(col) is str) and type(val) is str):
            reslist = table.loc[table[table.columns[col]] == val].values.tolist()
        else:
            reslist = table
            for c, v in zip(col, val):
                reslist = reslist.loc[reslist[reslist.columns[c]] == v]
            reslist = reslist.values.tolist()
        if(reslist):
            return reslist[0]
        else:
            return None

class DAG():
    def __init__(self, eta, eta_s, eta_t, xi, W, Table, idx):
        self.eta = eta 
        self.eta_s = eta_s 
        self.eta_t = eta_t 
        self.xi = xi 
        self.Table = Table
        self.idx = idx
        self.W = W # [SubStr(eta_s, token, num)]

    def estimated_output(self):
        return self.Concatenate()

    def print_constructor(self):
        #print("(DAG)", self.eta_s, self.eta_t, self.xi, self.W)
        if(not self.W):
            print('No constructor found')
        else:
            for item in self.W:
                if item.__class__.__name__ == "SubStr":
                    print("(DAG:W)", item.__class__.__name__, "(", item.String, ",", item.Token, ",", item.idx, ")")
                elif item.__class__.__name__ == "ConstStr":
                    print("(DAG:W)", item.__class__.__name__, "(", item.get_value(), ")")
                elif item.__class__.__name__ == "FirstStr":
                    print("(DAG:W)", item.__class__.__name__, "(", item.String, ")")
                elif item.__class__.__name__ == "MatchStr":
                    print("(DAG:W)", item.__class__.__name__, "(", item.get_value(), ")")
                elif item.__class__.__name__ == "Lookup":
                    print("(DAG:W)", item.__class__.__name__, "(", str(item.row), str(item.idx), item.Table, item.get_value(), ")")

    
    def Concatenate(self):
        outputString = ''
        for xis, Ws in zip(self.xi, self.W):
            outputString += Ws[-1].get_value()
        return outputString

    # def getMaxParts(self):
    #     maxParts = -1
    #     for xi in self.xi:
    #         if(xi[0] > maxParts):
    #             maxParts = xi[0]
    #     return maxParts

    # def Concatenate(self):
    #     outputString = ""
    #     maxlen = self.getMaxParts()
    #     outputParts = [None] * maxlen
    #     for xi, w in zip(self.xi, self.W):


    def get_input(self):
        if "" or None in self.eta_s:
            return None
        else:
            get_input = ""
            for part in self.eta_s:
                #print("part: ", part)
                get_input += part
            return get_input
        
    def __print_each_atom__(self, item):
        tempstr = ''
        if(type(item) is tuple or type(item) is list):
            for atom in item:
                tempstr += '(' + self.__print_each_atom__(atom) + ')'
        else:
            if item.__class__.__name__ == "SubStr":
                tempstr = "(DAG:W)" +  str(item.__class__.__name__) + "(" + str(item.String) + "," + str(item.Token) + "," + str(item.idx) + ")"
            elif item.__class__.__name__ == "ConstStr":
                tempstr = "(DAG:W)" +  str(item.__class__.__name__) + "(" + str(item.get_value()) + ")"
            elif item.__class__.__name__ == "FirstStr":
                tempstr = "(DAG:W)" +  str(item.__class__.__name__) + "(" + str(item.String) + ")"
            elif item.__class__.__name__ == "MatchStr":
                tempstr = "(DAG:W)" +  str(item.__class__.__name__) + "(" + str(item.get_value()) + ")"
            elif item.__class__.__name__ == "Lookup":
                tempstr = "(DAG:W)" + item.__class__.__name__ + "(" + str(item.row) + ', ' + str(item.String) + ', ' + str(item.Table) + ', ' + str(item.idx) + ', ' + str(item.get_value()) + ")"
                # tempstr = "(DAG:W)" + self.W[0].__class__.__name__ + "(" + str(self.Table) + str(self.idx) + str(1) + ")"
            else:
                tempstr = "Not found"
        return tempstr

    def __str__(self) -> str:
        printstr = ''
        printstr += 'eta: '
        printstr += str(self.eta)
        printstr += '\n eta_s: '
        printstr += str(self.eta_s)
        printstr += '\n eta_t: '
        printstr += str(self.eta_t)
        printstr += '\n xi: '
        printstr += str(self.xi)
        printstr += '\n Length of program: '
        printstr += str(len(self.W))
        printstr += '\n prog: '
        if(not self.W):
            tempstr = 'Not found'
            printstr += tempstr
        else:
            for item in self.W:
                tempstr = self.__print_each_atom__(item)
                if(tempstr):
                    printstr += tempstr
                else:
                    printstr += 'EMPTY STR!'
                printstr += '\n'
        printstr += 'Final Answer: '
        printstr += str(self.Concatenate())
        return printstr
    
    def __repr__(self) -> str:
        return self.__str__()

    
import copy
import time
import data.program as prog

from data.graph import validate_atoms, DAG



# Compare two expressions and sreturn 1 if two are identical and return 0 if not
def expr_comp(expr1, expr2):
	if expr1.id == expr2.id:
		if expr1.id == "SubStr" and expr1.Token == expr2.Token and expr1.num == expr2.num:
			return 1
		elif expr1.id == "ConstStr" and expr1.get_value() == expr2.get_value():
			return 1
		elif expr1.id == "FirstStr" and expr2.get_value() == expr2.get_value():
			return 1
		elif expr1.id == "MatchStr" and expr2.get_value() == expr2.get_value():
			return 1
		elif expr1.id == "Lookup" and expr1.Table == expr2.Table and expr1.idx == expr2.idx:
			return 1
		else:
			return 0
	else:
		return 0
	
def expr_comp_group(group1, group2):
	if((not type(group1) is tuple and not type(group1) is list) or (not type(group2) is tuple and not type(group2) is list)):
		return False
	if(not len(group1) == len(group2)):
		return False
	for expr1, expr2 in zip(group1, group2):
		res = expr_comp(expr1, expr2)
		if(not res):
			return False
	return True

def expr_comp_wrapper(op1, op2):
	if((type(op1) is tuple or type(op1) is list) and (type(op2) is tuple or type(op2) is list)):
		return expr_comp_group(op1, op2)
	elif(hasattr(op1, "id") and hasattr(op2, "id")):
		return expr_comp(op1, op2)
	else:
		return False

# Return generalized expression (which has no String)
def generalize_expr(expression):
	if expression.id == "SubStr":
		gen_expr = prog.SubStr(None, expression.Token, expression.num, expression.inputcol)
	elif expression.id == "FirstStr":
		gen_expr = prog.FirstStr(None)
	elif expression.id == "ConstStr":
		gen_expr = prog.ConstStr(expression.String, expression.pos)
	elif expression.id == "MatchStr":
		gen_expr = prog.MatchStr(None, expression.get_value())
	elif expression.id == "Lookup":
		gen_expr = prog.Lookup(None, expression.Table, expression.idx, expression.row)
	else:
		gen_expr = expression
	return gen_expr



# Return 1 if same one is found, 0 if unique
def comparator_all(classes_list, classes1):
	for classes2 in classes_list:
		if comparator(classes1, classes2):
			return 1
	return 0

# Compare two groups of classes and return 1 if same, 0 if different
def comparator(classes1, classes2):
	classes1_c = copy.deepcopy(classes1)
	classes2_c = copy.deepcopy(classes2)
	if len(classes1_c) != len(classes2_c):
		return 0
	else:
		for i in range(0, len(classes1_c)):
			for j in range(0, len(classes2_c)):
				if classes1_c[i] == classes2_c[j]:
					classes1_c.pop(i)
					classes2_c.pop(j)
					if len(classes1_c)==0 and len(classes2_c)==0:
						return 1
					else:
						return comparator(classes1_c, classes2_c)
		return 0



def lcss(lst1, lst2, verbose = False):
    m, n = len(lst1), len(lst2)
    jh = [[0 for j in range(n+1)] for i in range(m+1)]
    
    for i in range(1, m+1):
        for j in range(1, n+1):
            if(verbose):
                with open('lcss.txt', 'a') as f:
                    f.write('Comparing ' + str(lst1[i-1]) + ' and ' + str(lst2[j-1]) + '\n')
            if expr_comp_wrapper(lst1[i-1], lst2[j-1]):
                jh[i][j] = 1 + jh[i-1][j-1]
                if(verbose):
                    with open('lcss.txt', 'a') as f:
                        f.write('Equal \n')
            else:
                jh[i][j] = max(jh[i-1][j], jh[i][j-1])
                if(verbose):
                    with open('lcss.txt', 'a') as f:
                        f.write('Not equal \n')
    
    result1 = []
    result2 = []
    i, j = m, n
    while i > 0 and j > 0:
        # if lst1[i-1] == lst2[j-1]:
        if(expr_comp_wrapper(lst1[i-1], lst2[j-1])):
            result1.append(i - 1)
            result2.append(j - 1)
            i -= 1
            j -= 1
        elif jh[i-1][j] > jh[i][j-1]:
            i -= 1
        else:
            j -= 1
    
    return result1[::-1], result2[::-1]


def prog_comp(g1Dict, g2Dict, verbose = False):
	g1idxDict = dict()
	g2idxDict = dict()

	for g1xi, g1Ws in g1Dict.items():
		for g2xi, g2Ws in g2Dict.items():
			if(g1xi != g2xi):
				continue
			g1idx, g2idx = lcss(g1Ws, g2Ws, verbose)
			if(g1idx and g2idx):
				if(g1xi not in g1idxDict.keys()):
					g1idxDict[g1xi] = list()
				if(g2xi not in g2idxDict.keys()):
					g2idxDict[g2xi] = list()
				g1idxDict[g1xi] = g1idx
				g2idxDict[g2xi] = g2idx

	g1_common_xi = list()
	g2_common_xi = list()
	g1_common_W = list()
	g2_common_W = list()

	# edgeid: list(program)
	for g1xi, g1idx in g1idxDict.items():
		for idx in g1idx:
			g1_common_xi.append(g1xi)
			g1_common_W.append(g1Dict[g1xi][idx])
	for g2xi, g2idx in g2idxDict.items():
		for idx in g2idx:
			g2_common_xi.append(g2xi)
			g2_common_W.append(g2Dict[g2xi][idx])

	return g1_common_xi, g1_common_W, g2_common_xi, g2_common_W


def validate_each_output(xis, Ws, eta_s, eta_t, verbose = False):
	currt = -1
	currval = ''
	validateDict = {i: False for i in range(len(eta_t))}
	progdict = dict()
	for xi, w in zip(xis, Ws):
		# print(xi)
		# print(w)
		for edges, atoms in zip(xi, w):
			if(edges[1] not in progdict.keys()):
				progdict[edges[1]] = list()
			progdict[edges[1]].append(atoms)
	# print(progdict)
	for idx, progs in progdict.items():
		value = eta_t[idx]
		if(verbose):
			with open('validation_each.txt', 'a') as f:
				f.write('Current value: ' + str(value) + '\n')
		for prog in progs:
			currval = prog.get_value()
			if(verbose):
				with open('validation_each.txt', 'a') as f:
					f.write(str(prog) + '\n')
		if(verbose):
			with open('validation_each.txt', 'a') as f:
				f.write(str(currval) + '\n')
		if(currval == value):
			validateDict[idx] = True
	if(verbose):
		with open('validation_each.txt', 'a') as f:
			f.write(str(validateDict) + '\n')
			f.write('===========')
	finalvalidate = True
	for key, val in validateDict.items():
		if(not val):
			finalvalidate = False
			break
	return finalvalidate

def graphToDict(graph):
	gDict = dict()
	for xi, w in zip(graph.xis, graph.Ws):
		if(xi not in gDict.keys()):
			gDict[xi] = list()
		gDict[xi].append(w)

	return gDict

def graphToDict_single(graph):
	gDict = dict()
	for xi, w in zip(graph.xi, graph.W):
		if(xi not in gDict.keys()):
			gDict[xi] = list()
		gDict[xi].append(w)

	return gDict

def intersect_procedure(GraphDict, verbose = False):
	intertime = time.time()
	if(verbose):
		with open('intersection.txt', 'w') as f:
			f.write('Intersection procedure\n')
		with open('lcss.txt', 'w') as f:
			f.write('Finding lcss\n')
		with open('validation_each.txt', 'w') as f:
			f.write('Validation_each\n')
	GraphDict_new = dict()
	for outeridx, (iopair1, graphs1) in enumerate(GraphDict.items()):
		if(iopair1 not in GraphDict_new.keys()):
			GraphDict_new[iopair1] = list()
		for inneridx, (iopair2, graphs2) in enumerate(GraphDict.items()):
			if(inneridx < outeridx):
				continue
			if(verbose):
				with open('intersection.txt', 'a') as f:
					f.write('comparing ' + str(iopair1) + ' and ' + str(iopair2) + '\n')
			if(iopair1 == iopair2):
				if(verbose):
					with open('intersection.txt', 'a') as f:
						f.write('skipped\n')
				continue
				
			
			if(iopair2 not in GraphDict_new.keys()):
				GraphDict_new[iopair2] = list()


			for g1 in graphs1:
				g1Dict = graphToDict_single(g1)
				
				for g2 in graphs2:
					g2Dict = graphToDict_single(g2)
					if(not g1.xi == g2.xi):
						continue
					else:
						g1_common_xi, g1_common_W, g2_common_xi, g2_common_W = prog_comp(g1Dict, g2Dict, verbose)
						if(verbose):
							with open('lcss.txt', 'a') as f:
								f.write(str(g1_common_W) + '\n')
								f.write(str(g2_common_W) + '\n')
							with open('intersection.txt', 'a') as f:
								f.write('Intersection of ' + str(g1.W) + '\nand\n' + str(g2.W) + '\n')
								f.write(str(g1_common_W) + '\n')
								f.write(str(g2_common_W) + '\n')
								f.write('~~~~~~\n')
						if(validate_each_output(g1_common_xi, g1_common_W, g1.eta_s, g1.eta_t, verbose) and validate_each_output(g2_common_xi, g2_common_W, g2.eta_s, g2.eta_t, verbose)):
							if(verbose):
								with open('intersection.txt', 'a') as f:
									f.write('Output correct\n')
									f.write('~~~~~~\n')
							g1_new = DAG((g1.eta_s, g1.eta_t), g1.eta_s, g1.eta_t, g1_common_xi, g1_common_W, 0, 0)
							if(g1_new not in GraphDict_new[iopair1]):
								GraphDict_new[iopair1].append(g1_new)
							g2_new = DAG((g2.eta_s, g2.eta_t), g2.eta_s, g2.eta_t, g2_common_xi, g2_common_W, 0, 0)
							if(g2_new not in GraphDict_new[iopair2]):
								GraphDict_new[iopair2].append(g2_new)
						else:
							pass
							if(verbose):
								with open('intersection.txt', 'a') as f:
									f.write('Output wrong\n')
									f.write('~~~~~~\n')
				if(verbose):
					with open('intersection.txt', 'a') as f:
						f.write('==================\n')

	with open('graphdict_new.txt', 'w') as f:
		for key, val in GraphDict_new.items():
			f.write('Example pair: ' + str(key) + '\n')
			f.write(str(val) + '\n')
			f.write('==============\n')
	print('Time for intersection: ' + str(time.time() - intertime))

	return GraphDict_new
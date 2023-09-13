from __future__ import annotations
from abc import abstractmethod
from functools import total_ordering
from typing import Any, Callable, List, Union
from Levenshtein import distance
import numpy as np
from itertools import chain, combinations

from utils.lcs import lcs
import utils.tokens as tk
from utils.nonecheck import Nonecheck



class Match():
	def __init__(self, TokenSeq, num):
		self.TokenSeq = TokenSeq
		self.num = num
		self.id = "Match"

	def __eq__(self, other):
		if not isinstance(other, Match):
			return NotImplemented
		return (self.TokenSeq, self.num) == (other.TokenSeq, other.num)

	def __lt__(self, other):
		if not isinstance(other, Match):
			return NotImplemented
		return (self.TokenSeq, self.num) < (other.TokenSeq, other.num)

	def __str__(self) -> str:
		# output = ''
		# output += 'Token_withidx Program: ({}, {}, {}, {})'.format(self.tid, self.Token, self.num, self.matchtoken)
		return self.return_constructor()

	def __repr__(self) -> str:
		return self.__str__()

	def Conditional(self, String):
		if self.get_tokenseq_len(String) >= self.num:
			return True
		else:
			return False

	def get_tokenseq_len(self, String):
		# Divede string into regular expression
		nodes = Makenode(String, [])

		count = self.tokenseq_check(nodes, 0)
		return count

	def tokenseq_check(self, nodes, count):
		assert len(self.TokenSeq) > 0
		if len(self.TokenSeq) > len(nodes):
			return count
		else:
			flag = [0 for i in range(0, len(self.TokenSeq))]
			for i in range(0, len(self.TokenSeq)):
				node_tok = tk.identifyToken(nodes[i])
				if node_tok == self.TokenSeq[i]:
					flag[i] = 1
			if np.min(flag) == 1:
				count += 1
			nodes = nodes[1:]
			return self.tokenseq_check(nodes, count)

	def print_constructor(self):
		print("(Match)", self.__class__.__name__, "(", [i for i in self.TokenSeq], ",", self.num, ")")

	def return_constructor(self):
		TokenStr = ""
		for i in range(0, len(self.TokenSeq)):
			TokenStr += str(self.TokenSeq[i])
			TokenStr += ", "
		print(TokenStr)
		print(type(TokenStr))
		string = "(ATOM)" + self.__class__.__name__ + "(" + TokenStr + "," + str(self.num) + ")"
		return string

class Conjunction():
	def __init__(self, Matches):
		self.Matches = Matches
		self.id = "Conjunction"

	def __eq__(self, other):
		if not isinstance(other, Conjunction):
			return NotImplemented
		return (self.Matches) == (other.Matches)

	def __lt__(self, other):
		if not isinstance(other, Conjunction):
			return NotImplemented
		return (self.Matches) < (other.Matches)

	def __str__(self) -> str:
		# output = ''
		# output += 'Conjunction({}, {}, {}, {})'.format(self.tid, self.Token, self.num, self.matchtoken)
		return str(self.return_constructor())

	def __repr__(self) -> str:
		return self.__str__()

	def Conditional(self, String):
		for Match in self.Matches:
			if not Match.Conditional(String):
				return False
		return True

	def print_constructor(self):
		print("(Conjunction)", self.__class__.__name__)
		for Match in self.Matches:
			Match.print_constructor()

	def return_constructor(self):
		print("(Conjunction)", self.__class__.__name__)
		constructors = list()
		for Match in self.Matches:
			constructors.append(Match.return_constructor())
		return constructors

class Token_withidx():
	def __init__(self, tid, Token, num, matchtoken=None):
		self.tid = tid
		self.Token = Token
		self.num = num
		self.matchtoken = matchtoken

	def __eq__(self, other):
		if not isinstance(other, Token_withidx):
			return NotImplemented
		return (self.tid, self.Token, self.num, self.matchtoken) == (other.tid, other.Token, other.num, other.matchtoken)

	def __lt__(self, other):
		if not isinstance(other, Token_withidx):
			return NotImplemented
		return (self.tid, self.Token, self.num, self.matchtoken) < (other.tid, other.Token, other.num, other.matchtoken)

	def __str__(self) -> str:
		output = ''
		output += 'Token_withidx Program: ({}, {}, {}, {})'.format(self.tid, self.Token, self.num, self.matchtoken)
		return output

	def __repr__(self) -> str:
		return self.__str__()

def index_multi(l, x):
    return [i for i, _x in enumerate(l) if _x == x]

def get_indices(tokens, token_withidxs):
	Tokenname_list = ['AlphaTok', 'NumTok', 'SpaceTok', 'PeriodTok', 'CommaTok', 'LeftParenthesisTok', 'RightParenthesisTok', 
						'SQuoteTok', 'DQuoteTok', 'HyphenTok', 'UBarTok', 'SlashTok', 'NoneTok']

	for Tokenname in Tokenname_list:
		indices = index_multi(tokens, Tokenname)
		for n, idx in enumerate(indices):
			token_withidxs[idx].num = n
	return 0


def make_token_withidx(nodes):
	tokens = list()
	token_withidx = list()
	for i, node in enumerate(nodes):
		token = tk.identifyToken(node) #"AlphaToken"
		assert token != -1
		tokens.append(token)
		token_withidx.append(Token_withidx(i, token, -1))

	get_indices(tokens, token_withidx)

	return token_withidx

@total_ordering
class SubStr():
	def __init__(self, String, Token, num, idx, src):
		if(type(String) is str):
			self.String = String
		elif((type(self.String) is list or type(self.String) is tuple) and len(self.String) == 1):
			self.String = String[0]
		self.Token = Token
		self.num = num
		self.id = "SubStr"
		self.src = src
		self.idx = idx

	def __eq__(self, other):
		if not isinstance(other, SubStr):
			return NotImplemented
		return (self.String, self.Token, self.num, self.id) == (other.String, other.Token, other.num, other.id)
	
	def __hash__(self):
		return hash((self.String, self.Token, self.num, self.id))

	def __lt__(self, other):
		if not isinstance(other, SubStr):
			return NotImplemented
		return (self.String, self.Token, self.num, self.id) < (other.String, other.Token, other.num, other.id)

	def __str__(self) -> str:
		output = ''
		output += 'SubStr Program: SubStr({}, {}, {}), input column {}'.format(self.String, self.Token, self.num, self.inputcol)
		return output

	def __repr__(self) -> str:
		return self.__str__()

	# def get_value(self):
	# 	#print("string, token, num", self.String, self.Token, self.num)
	# 	if Nonecheck(self.String):
	# 		return ""
	# 		# if(Nonecheck(self.inputcol)):
	# 		# 	return ""
	# 		# else:
	# 		# 	self.String = 
	# 	else:
	# 		#print(self.String, self.Token, self.num)
	# 		try:
	# 			x = self.getnode(self.String, self.Token)[self.num]
	# 			return str(x)
	# 		except:
	# 			return None
	def get_value(self):
		# print(self.src)
		if(type(self.src) is list or type(self.src) is tuple):
			src = self.src[0]
		if(len(self.String) < src):
			return ""
		if((type(self.String) is list or type(self.String) is tuple) and len(self.String) == 1):
			self.String = self.String[0]
		if(not self.String):
			return ""
		else:
			# print(self.String)
			# print()
			# print(self.idx)
			# res = self.getnode(str(self.String.split()[self.idx]), self.Token)
			res = str(self.String.split()[self.idx])
			# print(res)
			return res

	def getnode(self, String, Token):
		if Token == "NumTok":
			node = tk.NumTok(String)
		elif Token == "AlphaTok":
			node = tk.AlphaTok(String)
		elif Token == "NoneTok":
			node = tk.NoneTok(String)
			print("NoneTok", node)
		elif Token == "StartTok":
			node = tk.StartTok(String)
		elif Token == "EndTok":
			node = tk.EndTok(String)
		else:
			return -1
		return node

	def print_constructor(self):
		print("(ATOM)", self.__class__.__name__, "(", self.String, ",", self.Token, ",", self.num, ")")

	def return_constructor(self):
		inp = self.String
		if self.String == None:
			inp = "None"

		string = "(ATOM)" + self.__class__.__name__ + "(" + inp + "," + self.Token + "," + str(self.num) + "), representing input column" + str(self.inputcol)
		return string

@total_ordering
class ConstStr():
	def __init__(self, String, pos):
		self.String = String
		self.pos = pos
		self.id = "ConstStr"

	def __eq__(self, other):
		if not isinstance(other, ConstStr):
			return NotImplemented
		return (self.String, self.id) == (other.String, other.id)\
	
	def __hash__(self):
		return hash((self.String, self.id))

	def __lt__(self, other):
		if not isinstance(other, ConstStr):
			return NotImplemented
		return (self.String, self.id) < (other.String, other.id)
	
	def __str__(self) -> str:
		output = ''
		output += 'ConstStr Program: ConstStr({}, {})'.format(self.String, self.pos)
		return output

	def __repr__(self) -> str:
		return self.__str__()

	def get_value(self):
		return str(self.String)

	def print_constructor(self):
		print("(ATOM)", self.__class__.__name__, "(", self.get_value(), str(self.pos), ")")

	def return_constructor(self):
		string = "(ATOM)" + self.__class__.__name__ + "(" + self.get_value() + str(self.pos) + ")"
		return string

@total_ordering
class FirstStr():
	def __init__(self, String):
		self.String = String
		self.id = "FirstStr"

	def __eq__(self, other):
		if not isinstance(other, FirstStr):
			return NotImplemented
		return (self.String, self.id) == (other.String, other.id)
	
	def __hash__(self):
		return hash((self.String, self.id))

	def __lt__(self, other):
		if not isinstance(other, FirstStr):
			return NotImplemented
		return (self.String, self.id) < (other.String, other.id)

	def __str__(self) -> str:
		output = ''
		output += 'FirstStr Program: FirstStr({})'.format(self.String)
		return output

	def __repr__(self) -> str:
		return self.__str__()

	def get_value(self):
		#print("(FirstStr)", self.String)
		if Nonecheck(self.String):
			return self.String
		else:
			return self.String[0]

	def print_constructor(self):
		print("(ATOM)", self.__class__.__name__, "(", self.String, ")")

	def return_constructor(self):
		string = "(ATOM)" + self.__class__.__name__ + "(" + self.String + ")"
		return string


@total_ordering
class MatchStr():
	def __init__(self, String, Token):
		self.String = String
		self.Token = Token
		self.id = "MatchStr"

	def __eq__(self, other):
		if not isinstance(other, MatchStr):
			return NotImplemented
		return (self.String, self.Token, self.id) == (other.String, other.Token, other.id)
	
	def __hash__(self):
		return hash((self.String, self.Token, self.id))

	def __lt__(self, other):
		if not isinstance(other, MatchStr):
			return NotImplemented
		return (self.String, self.Token, self.id) < (other.String, other.Token, other.id)

	def __str__(self) -> str:
		output = ''
		output += 'MatchStr Program: MatchStr({}, {})'.format(self.String, self.Token)
		return output

	def __repr__(self) -> str:
		return self.__str__()

	def get_value(self):
		#print("string, token, num", self.String, self.Token, self.num)
		if Nonecheck(self.String):
			return self.String
		else:
			if self.Token in self.String:
				return str(self.Token)

	def print_constructor(self):
		print("(ATOM)", self.__class__.__name__, "(", self.get_value(), ")")

	def return_constructor(self):
		string = "(ATOM)" + self.__class__.__name__ + "(" + self.get_value() + ")"
		return string
	
@total_ordering
class Lookup():
	def __init__(self, String, Table, idx, row, src):
		if(type(String) is list):
			self.String = tuple(String)
		else:
			self.String = String
		self.Table = Table
		self.idx = idx
		self.row = row
		self.src = src
		self.id = "Lookup"
		# self.fromcol = self.row.index(self.String)
		if(type(self.String) is str):
			self.fromcol = self.row.index(self.String)
		if(type(self.String) is list or type(self.String) is set or type(self.String) is tuple):
			self.fromcol = list()
			for item in self.String:
				self.fromcol.append(self.row.index(item))

	def __eq__(self, other):
		if not isinstance(other, Lookup):
			return NotImplemented
		return (self.String, self.Table, self.idx, self.id) == (other.String, other.Table, self.idx, other.id)
	
	def __hash__(self):
		return hash((self.String, self.Table, self.idx, self.id))

	def __lt__(self, other):
		if not isinstance(other, Lookup):
			return NotImplemented
		return (self.String, self.Table, self.idx, self.id) < (other.String, other.Table, self.idx, other.id)

	def __str__(self) -> str:
		output = ''
		output += 'Lookup Program: Lookup({}, {}, {}, {})'.format(str(self.row), self.String, self.Table, self.idx)
		return output

	def __repr__(self) -> str:
		return self.__str__()

	def get_value(self):
		# if Nonecheck(self.String):
		if(self.row):
			res =  self.row[self.idx]
		else:
			res = ''
		# print(self.idx, self.Table, res)
		return str(res)


	def print_constructor(self):
		print("(ATOM)", self.__class__.__name__, "(", self.get_value(), ")")

	def return_constructor(self):
		string = "(ATOM)" + self.__class__.__name__ + "(" + self.get_value() + ")"
		return string
	

def Makenode(String, Nodes):
	#print("(Node) String: ", String)
	Token = tk.identifyToken(String)
	node = tk.getnode(String, Token)
	#print("(Node) state: ", String, Token, node)
	if node == "":
		return Nodes
	elif Nonecheck(node):
		Nodes.append(node)
		return Nodes
	else:
		Nodes.append(node)
		String = String[len(str(node)):]
		return Makenode(String, Nodes)

def powerset(iterable):
    "powerset([1,2,3]) --> (1,) (2,) (3,) (1,2) (1,3) (2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(1, len(s) - 1))

def Makenode_comb(String, Nodes):
	nodes = Makenode(String, Nodes)
	nodes = powerset(nodes)
	# print(list(nodes))
	strlist = set()
	for item in nodes:
		listednodes = [i for i in item if i != ' ' and type(i) is not int]
		currstr = ' '.join(list(listednodes))
		if(currstr not in strlist and currstr != ''):
			strlist.add(currstr)
	print(strlist)
	return list(strlist)

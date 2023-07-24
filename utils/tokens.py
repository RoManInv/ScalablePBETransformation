import sys
import re

from utils.nonecheck import Nonecheck

def Makenode(String, Nodes):
	#print("(Node) String: ", String)
	Token = identifyToken(String)
	node = getnode(String, Token)
	#print("(Node) state: ", String, Token, node)
	if node == "":
		return Nodes
	elif Nonecheck(node):
		Nodes.append(node)
		return Nodes
	else:
		Nodes.append(node)
		String = String[len(node):]
		return Makenode(String, Nodes)


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

def getnode(String, Token):
	if Token == "EOFTok":
		node = EOFTok(String)
	elif Token == "DateTok":
		node = [DateTok(String)]
	elif Token == "SpaceTok":
		node = SpaceTok(String)
	elif Token == "PeriodTok":
		node = PeriodTok(String)
	elif Token == "CommaTok":
		node = CommaTok(String)
	elif Token == "LeftParenthesisTok":
		node = LeftParenthesisTok(String)
	elif Token == "RightParenthesisTok":
		node = RightParenthesisTok(String)
	elif Token == "DQuoteTok":
		node = DQuoteTok(String)
	elif Token == "SQuoteTok":
		node = SQuoteTok(String)
	elif Token == "HyphenTok":
		node = HyphenTok(String)
	elif Token == "UBarTok":
		node = UBarTok(String)
	elif Token == "SlashTok":
		node = SlashTok(String)
	elif Token == "NoneTok":
		node = NoneTok(String)
	elif Token == "NumTok":
		node = NumTok(String)
	elif Token == "AlphaTok":
		node = AlphaTok(String)
	elif Token == "PercentageTok":
		node = PercentageTok(String)
	elif Token == "AddTok":
		node = AddTok(String)
	elif Token == "SubstractTok":
		node = SubstractTok(String)
	elif Token == "MultiplyTok":
		node = MultiplyTok(String)
	elif Token == "DollarTok":
		node = DollarTok(String)
	else:
		print("getnode didn't match any token: ", String)
		return -1
	return node[0]

alphaReg = re.compile(r'^[a-zA-Z]+$')
def isalpha(s):
    return alphaReg.match(s) is not None

numReg = re.compile(r'[-+]?(?:\d*\.*\d+)')
def isnumordecimal(s):
	return numReg.match(s) is not None

def DateTok(string):
	node = re.findall(r"(0?[1-9]|[1-3][0-9]\/)?([0]{0,1}[1-9][11,12]\/)(\d{4})", string)
	if(node):
		node = list(node[0])
		node = ''.join(node)
		return node
	else:
		return None

def NumTok(string):
	# node = re.findall(r"[0-9]+" , string)
	node = re.findall(r'[-+]?(?:\d*\.*\d+)', string)
	return node

def AlphaTok(string):
	node = re.findall(r"[a-zA-Z]+" , string)
	return node

def SpaceTok(string):
	node = [" "]
	return node

def PeriodTok(string):
	node = ["."]
	return node

def CommaTok(string):
	node = [","]
	return node

def LeftParenthesisTok(string):
	node = ["("]
	return node

def RightParenthesisTok(string):
	node = [")"]
	return node

def DQuoteTok(string):
	node = ["\""]
	return node

def SQuoteTok(string):
	node = ["'"]
	return node

def HyphenTok(string):
	node = ["-"]
	return node

def UBarTok(string):
	node = ["_"]
	return node

def SlashTok(string):
	node = ["/"]
	return node

def StartTok(string):
	node = string[0]
	return node

def EndTok(string):
	node = string[-1]
	return node

def EOFTok(string):
	node = [""]
	return node

def NoneTok(string):
	node = [None]
	return node

def MatchTok(string):
	node = [string]
	return node

def PercentageTok(string):
	node = ['%']
	return node

def AddTok(string):
	node = ["+"]
	return node

def SubstractTok(string):
	node = ["-"]
	return node

def MultiplyTok(string):
	node = ["*"]
	return node

def DollarTok(string):
	node = ["$"]
	return node


def identifyToken(String):
	if String == "":
		token = "EOFTok"
	elif Nonecheck(String):
		token = "NoneTok"
	else:
		firstchar = String[0]
		# if firstchar.isdecimal():
		if DateTok(String):
			token = "DateTok"
		elif isnumordecimal(firstchar):
			token = "NumTok"
		elif isalpha(firstchar):
			token = "AlphaTok"
		elif firstchar == " ":
			token = "SpaceTok"
		elif firstchar == ".":
			token = "PeriodTok"
		elif firstchar == ",":
			token = "CommaTok"
		elif firstchar == "(":
			token = "LeftParenthesisTok"
		elif firstchar == ")":
			token = "RightParenthesisTok"
		elif firstchar == "\"":
			token = "DQuoteTok"
		elif firstchar == "'":
			token = "SQuoteTok"
		elif firstchar == "-":
			token = "HyphenTok"
		elif firstchar == "_":
			token = "UBarTok"
		elif firstchar == "/":
			token = "SlashTok"
		elif firstchar == "%":
			token = "PercentageTok"
		elif firstchar == "+":
			token = "AddTok"
		elif firstchar == "-":
			token = "SubstractTok"
		elif firstchar == "*":
			token = "MultiplyTok"
		elif firstchar == '$':
			token = "DollarTok"
		else:
			print("identifyToken couldn't find any token. Input: ", firstchar)
			return -1
		# print(token)
	return token
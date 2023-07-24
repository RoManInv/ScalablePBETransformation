import math

def Nonecheck(Input):
	if type(Input) is tuple and Input:
		return True
	if type(Input) != str and (Input == None or math.isnan(Input)):
		return True
	elif type(Input) == list and not Input:
		return True
	else:
		return False
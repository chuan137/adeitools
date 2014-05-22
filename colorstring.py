HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
C_ERR = '\033[91m'
C_WRN = '\033[94m'
C_END = '\033[0m'

def ERROR(s):
   return C_ERR + s + C_END 

def WARNING(s):
    return C_WRN + s + C_END



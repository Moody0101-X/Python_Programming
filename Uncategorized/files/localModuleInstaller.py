from sys import argv
from colors import *

doc = f"""

{YELLOW}
usage: localModuleInstaller.py module PythonSitePackagesPath
{GREEN}
module: the path of the Module that you want to access locally using an import statement.
PythonSitePackagesPath: the diractory that houses python site-packages
if not specified this will be used as a defaul
{RESET}

"""


DEFAULTPATH = "C:\\Users\\pc\\AppData\\Local\\Programs\\Python\\Python310\\Lib\\site-packages"



if len(argv) > 1:
	from shutil import copy
	if len(argv) == 2:
		copy(argv[1], DEFAULTPATH)
	else:
		copy(argv[1], argv[3])

else:
	print(doc)
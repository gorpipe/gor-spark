import sys
import os
import papermill as pm
ipynbPath = sys.argv[1]
outputPath = sys.argv[2]
params = {}
for i in range(3, len(sys.argv), 2):
    params[sys.argv[i][2:]] = sys.argv[i+1]
#os.makedirs(outputPath, exist_ok=True)
pm.execute_notebook(ipynbPath,outputPath,parameters=params,)
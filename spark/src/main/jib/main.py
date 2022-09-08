import sys
import os
import papermill as pm
projectPath = sys.argv[2]
projectName = sys.argv[4]
outputPath = sys.argv[6]
ipynbPath = projectPath + '/' + sys.argv[8]
outputPath = projectPath + '/' + outputPath + '/' + sys.argv[10]
params = {}
for i in range(1, len(sys.argv), 2):
    params[sys.argv[i][2:]] = sys.argv[i+1]
#os.makedirs(outputPath, exist_ok=True)
pm.execute_notebook(ipynbPath,outputPath,parameters=params,)
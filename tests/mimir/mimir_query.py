import json
import os
import vistrails.packages.mimir.init as mimir

#
# CSV__FILE - dataset.csv
#
#Name,Age,Salary
#Alice,23,35K
#Bob,32,30K

CSV_FILE = '../data/mimir/dataset.csv'

include_uncertainty = True
include_reasons = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE), ',', True, True)

sql = 'SELECT * FROM ' + table_name
jstr = mimir._mimir.vistrailsQueryMimirJson(sql, include_uncertainty, include_reasons)
#for i in range(40):
#    pos = 49764478 + i
#    c = jstr[pos]
#    print str(pos) + '\t' + str(c) + '\t' + repr(c) + '\t' + str(ord(c))
#with open('JSONOUTPUTWIDE.json', 'w') as f:
#    f.write(jstr.encode('utf-8'))
rs = json.loads(jstr.replace('\t', ' '))

print json.dumps(rs, indent=4, sort_keys=True)

schema = json.loads(mimir._mimir.getSchema(sql))
print schema

mimir.finalize()

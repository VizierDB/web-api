import os
import vistrails.packages.mimir.init as mimir

#
# CSV__FILE - dataset.csv
#
#Name,Age,Salary
#Alice,23,35K
#Bob,32,30K

CSV_FILE = '../data/dataset.csv'

include_uncertainty = False
include_reasons = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

sql = 'SELECT * FROM ' + table_name
rs = mimir._mimir.vistrailsQueryMimirJson(sql, include_uncertainty, include_reasons)

print rs

mimir.finalize()

import os
import vistrails.packages.mimir.init as mimir

#
# CSV__FILE - dataset.csv
#
#Name,Age,Salary
#Alice,23,35K
#Bob,32,30K

mimir.initialize()

sql = 'SELECT * FROM LENS_KEY_REPAIR881751034'
csvStrDet = mimir._mimir.vistrailsQueryMimir(sql, True, True)
print csvStrDet.csvStr()

mimir.finalize()

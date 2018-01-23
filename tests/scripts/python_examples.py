from vizier.client.server import VizierClient
from vizier.client.command import VizualLoad, VizualUpdateCell, PythonCmd

vizier = VizierClient()

prj = vizier.projects_create(name='Two dataset project')
ds = vizier.datasets_upload("../tests/data/dataset.csv")

#
# 1
#
result = prj.append_command(VizualLoad(uri=ds.uri, name='people'))
for line in result['outputs']:
    print line['text']


#
# 2
#
py_list_names = """from vizier.client.database import VizierDBClient
vizier = VizierDBClient(__vizierdb__)
ds = vizier.get_dataset('people')
for row in ds.rows:
    print row.get_value('Name')
"""

result = prj.append_command(PythonCmd(content=py_list_names))
for line in result['outputs']:
    print line['text']

#
# 3
#
result = prj.append_command(PythonCmd(content="""names={'Alice' : 'Sue', 'Bob' : 'Mark'}"""))
module_id = result['moduleId']
for line in result['outputs']:
    print line['text']


#
# 4
#
py_update_names = """from vizier.client.database import VizierDBClient
vizier = VizierDBClient(__vizierdb__)
ds = vizier.get_dataset('people')
for row in ds.rows:
    row.set_value('Name', names[row.get_value('Name')])
vizier.update_dataset('people', ds)
"""

result = prj.append_command(PythonCmd(content=py_update_names))
for line in result['outputs']:
    print line['text']


#
# 5
#
py_list_names = """from vizier.client.database import VizierDBClient
vizier = VizierDBClient(__vizierdb__)
dataset = vizier.get_dataset('people')
for row in dataset.rows:
    print row.get_value(0)
"""

result = prj.append_command(PythonCmd(content=py_list_names))
for line in result['outputs']:
    print line['text']

#
# 6
#
result = prj.append_command(VizualUpdateCell(dataset='people', column=0, row=1, value='Paul'), before_id=module_id)
for line in result['outputs']:
    print line['text']

#
# 7
#
result = prj.replace_command(PythonCmd(content="""names={'Alice' : 'Sue', 'Paul' : 'Mark'}"""), module_id)
for line in result['outputs']:
    print line['text']

#
# 8
#
ds = vizier.datasets_upload("../tests/data/mel-features.tsv")
result = prj.append_command(VizualLoad(uri=ds.uri, name='melbourne'))
for line in result['outputs']:
    print line['text']

#
# 9
#
py_list_frequent_features = """from vizier.client.database import VizierDBClient
vizier = VizierDBClient(__vizierdb__)
ds = vizier.get_dataset('melbourne')
features = {}
for row in ds.rows:
    for token in row.get_value('featurenam').split():
        if token in features:
            features[token] += 1
        else:
            features[token] = 1
f_sorted = sorted(features.keys(), key=lambda f: features[f], reverse=True)
for i in range(10):
    print f_sorted[i] + ' ' + str(features[f_sorted[i]])
"""

result = prj.append_command(PythonCmd(content=py_list_frequent_features))
for line in result['outputs']:
    print line['text']

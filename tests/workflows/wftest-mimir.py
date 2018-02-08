"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil

import vistrails.packages.mimir.init as mimir

from vizier.config import ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_DEFAULT, ENGINEENV_MIMIR
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.workflow.command as cmd

DATASTORE_DIR = '../env/ds'
FILESERVER_DIR = '../env/fs'
VIZTRAILS_DIR = '../env/vt'

CSV_FILE = '../data/dataset.csv'

DS_NAME = 'people'

def list_ds_dir():
    print
    print 'DATASTORE:'
    print '----------'
    for f in os.listdir(DATASTORE_DIR):
        print f
    print

for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
    if os.path.isdir(d):
        shutil.rmtree(d)
    os.mkdir(d)

env = ExecEnv(
        FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
        identifier=ENGINEENV_MIMIR
    ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
ENGINE_ID = env.identifier

mimir.initialize()
datastore = MimirDataStore(DATASTORE_DIR)
fileserver = DefaultFileServer(FILESERVER_DIR)
db = FileSystemViztrailRepository(VIZTRAILS_DIR, {ENGINE_ID: env})

f_handle = fileserver.upload_file(CSV_FILE)
vt = db.create_viztrail(ENGINE_ID, {'name' : 'My Project'})
print '(1) CREATE DATASET'
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.load_dataset(f_handle.identifier, DS_NAME)
)
print
print '(2) INSERT ROW'
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.insert_row(DS_NAME, 1)
)
print
print '(3) SET NAME IN NEW ROW'
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.update_cell(DS_NAME, 'Name', 1, 'Claire')
)
print
print '(4) CREATE FRIENDS'
code = """
from vizier.datastore.base import Dataset
ds = Dataset()
ds.add_column('NickName')
for row in vizierdb.get_dataset('people').rows:
    ds.add_row([row.get_value('Name')])
vizierdb.create_dataset('friends', ds)
"""
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.python_cell(code)
)
print
print '(5) DELETE ROW'
db.append_workflow_module(
    viztrail_id=vt.identifier,
    command=cmd.delete_row(DS_NAME, 1)
)
print
print '(6) DELETE MODULE'
db.delete_workflow_module(
    viztrail_id=vt.identifier,
    module_id=1
)

list_ds_dir()

print '\nRESULT\n-------'
wf = db.get_workflow(viztrail_id=vt.identifier)

ds = datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
print [c.name for c in ds.columns]
for row in ds.rows:
    print row.values
print
ds = datastore.get_dataset(wf.modules[-1].datasets['friends'])
print [c.name for c in ds.columns]
for row in ds.rows:
    print row.values

mimir.finalize()

import logging
import os
import shutil

#import vistrails.core.db.io
#from vistrails.core.system import vistrails_version
from vistrails.core.db.locator import BaseLocator, UntitledLocator
from vistrails.core.vistrail.controller import VistrailController
import vistrails.core.application
from vistrails.core.db.action import create_action
#from vistrails.core.modules.module_registry import get_module_registry
from vistrails.core.modules.vistrails_module import ModuleError
from vistrails.core.packagemanager import get_package_manager

from vizier.config import TestEnv
from vizier.datastore.mem import InMemDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.command import python_cell, load_dataset, mimir_missing_value
from vizier.workflow.command import MODTYPE_PYTHON, MODTYPE_MIMIR, MODTYPE_VIZUAL
from vizier.workflow.context import VZRENV_VARS_DBCLIENT, WorkflowContext
from vizier.workflow.vizual.base import DefaultVizualEngine

from vizier.workflow.packages.userpackages.vizierpkg import PythonCell



def create_module(command, controller):
    ops = list()
    if command.is_type(MODTYPE_MIMIR):
        # Create a new Mimir Lens
        module = controller.create_module(
            'org.vistrails.vistrails.vizier',
            'MimirLens'
        )
        ops.append(('add', module))
        # Set the name and arguments parameter
        ops.extend(
            controller.update_function_ops(
                module,
                'name',
                [command.command_identifier]
            )
        )
        ops.extend(
            controller.update_function_ops(
                module,
                'arguments',
                [command.arguments]
            )
        )
    elif command.is_type(MODTYPE_PYTHON):
        # Create a new Python Cell
        module = controller.create_module(
            'org.vistrails.vistrails.vizier',
            'PythonCell'
        )
        ops.append(('add', module))
        # Set the source parameter
        ops.extend(
            controller.update_function_ops(
                module,
                'source',
                [command.arguments['source']]
            )
        )
    elif command.is_type(MODTYPE_VIZUAL):
        # Create a new Vizual Cell
        module = controller.create_module(
            'org.vistrails.vistrails.vizier',
            'VizualCell'
        )
        ops.append(('add', module))
        # Set the name and arguments parameter
        ops.extend(
            controller.update_function_ops(
                module,
                'name',
                [command.command_identifier]
            )
        )
        ops.extend(
            controller.update_function_ops(
                module,
                'arguments',
                [command.arguments]
            )
        )
    else:
        raise ValueError('unknown module type \'' + command.module_type + '\'')
    return module, ops

def do_ops(ops, controller):
    action = create_action(ops)
    controller.add_new_action(action)
    version = controller.perform_action(action)
    controller.change_selected_version(version)
    locator = BaseLocator.from_url(os.path.join(VIZTRAIL_DIR, VIZTRAIL_FILE))
    controller.flush_delayed_actions()
    controller.write_vistrail(locator)
    return version


def execute(version, controller):
    """Executes a given version and return the results.
    """
    controller.change_selected_version(version)
    pipeline = controller.current_pipeline
    results, changed = controller.execute_workflow_list([[
        controller.locator,  # locator
        version,  # version
        pipeline,  # pipeline
        None,  # view
        None,  # custom_aliases
        None,  # custom_params
        None,  # reason
        None,  # sinks
        None,  # extra_info
    ]])
    result, = results
    outputs = {}
    for module in pipeline.module_list:
        out = get_outputs(result.objects[module.id])
        if out:
            outputs[module.id] = out
    errors = dict(
        (module_id,
        dict(type='pyexception', exctype=type(error).__name__,
                         message=error.message))
        for module_id, error in result.errors.iteritems()
    )
    return outputs, errors

def get_outputs( obj):
    try:
        return obj.get_output('output')
    except (ModuleError, KeyError):
        logger.info("KEY ERROR")
        return []

def insert_module(module, ops, context, controller, before_module=-1):
    pipeline = controller.current_pipeline
    if before_module < 0:
        sinks = pipeline.graph.sinks()
        assert len(sinks) <= 1
        if sinks:
            last_mod = pipeline.modules[sinks[0]]
            up_conn = controller.create_connection(
                last_mod,
                'context',
                module,
                'context'
            )
            ops.append(('add', up_conn))
        else:
            ops.extend(
                controller.update_function_ops(
                    module,
                    'context',
                    [context]
                )
            )


logging.basicConfig()
logger = logging.getLogger(__name__)


"""Files containing viztrail information."""
FILESERVER_DIR = '.env/fs'

VISTRAIL_DOT = '.env/.vt'

VIZTRAIL_FILE = 'vistrail.vt'
VIZTRAIL_DIR = '.env/vt'

""" Clear directories """
if os.path.isdir(FILESERVER_DIR):
    shutil.rmtree(FILESERVER_DIR)
if os.path.isdir(VISTRAIL_DOT):
    shutil.rmtree(VISTRAIL_DOT)
if os.path.isdir(VIZTRAIL_DIR):
    shutil.rmtree(VIZTRAIL_DIR)
os.makedirs(VIZTRAIL_DIR)

"""Initialize Vistrails."""
logger = logging.getLogger()

vistrails_app = vistrails.core.application.init(
    options_dict={
        # Don't try to install missing dependencies
        'installBundles': False,
        # Don't enable all packages on start
        'loadPackages': False,
        # Enable packages automatically when they are required
        'enablePackagesSilently': True,
        # Load additional packages from there
        'userPackageDir': os.path.join(os.getcwd(), '/home/heiko/projects/vizier/vizier-webapi/vizier/workflow/packages/userpackages'),
        # Set dotVistrailes for local server
        'dotVistrails': VISTRAIL_DOT,
    },
    args=[]
)

get_package_manager().late_enable_package('vizierpkg')

locator = UntitledLocator()
loaded_objs = vistrails.core.db.io.load_vistrail(locator)
controller = VistrailController(
    loaded_objs[0],
    locator,
    *loaded_objs[1:]
)
locator = BaseLocator.from_url(os.path.join(VIZTRAIL_DIR, VIZTRAIL_FILE))
controller.flush_delayed_actions()
controller.write_vistrail(locator)

fileserver = DefaultFileServer(FILESERVER_DIR)
datastore = InMemDataStore()
context = WorkflowContext(TestEnv())
print context

code1 = """from vizier.datastore.mem import InMemDataStore
ds = InMemDataStore()
"""
controller.change_selected_version(0)
(module, ops) = create_module(python_cell(code1), controller)
insert_module(module, ops, context, controller)
version = do_ops(ops, controller)
outputs, errors = execute(version, controller)

controller.change_selected_version(version)
(module, ops) = create_module(python_cell('print ds'), controller)
insert_module(module, ops, context, controller)
version = do_ops(ops, controller)
outputs, errors = execute(version, controller)

print outputs
print errors

cell = PythonCell()
print cell.moduleInfo

print 'DONE'

"""Vistrails-type engine."""

from vizier.datastore.mem import VolatileDataStore
from vizier.workflow.module import ModuleHandle
from vizier.workflow.packages.userpackages.vizierpkg import MimirLens, PythonCell, VizualCell
from vizier.workflow.context import WorkflowContext
from vizier.workflow.engine.base import WorkflowExecutionResult, WorkflowEngine
from vizier.workflow.engine.base import TXT_NORMAL, TXT_ERROR

import vizier.config as config
import vizier.workflow.command as cmdtype
import vizier.workflow.context as ctx


class DefaultViztrailsEngine(WorkflowEngine):
    """Implementation of the workflow engine using Vistrails modules but not
    the Vistrails controller for workflow execution.
    """
    def __init__(self, exec_env):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        exec_env: vizier.config.ExecEnv
            Environment for execution of viztrail workflows
        """
        self.exec_env = exec_env

    def copy_workflow(self, version, modules):
        """Make a copy of the given workflow up until the given module
        (including). If module identifier is negative the complete workflow is
        copied.

        Parameters
        ----------
        version: int
            Unique version identifier for new workflow
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        # Return a result that contains the next version identifier and a copy
        # of all modules in the module list. The modified module identifier is
        # the identifier of the first module (reflecting that all modules have
        # been copied and executed for the new branch).
        # In this implementation the modules are only copied and not executed
        # since the workflow inputs and outputs should not change.
        return WorkflowExecutionResult(
            version,
            modules[0].identifier,
            [m.copy() for m in modules]
        )

    def execute_module(self, module, context):
        """Execute a given workflow module. Depending on the module command type
        a corresponding Viztrails cell is created and the compute method called.
        Returns a handle to the executed module.

        Parameters
        ----------
        module: vizier.workflow.module.ModuleHandle
            Handle for the input module. A new handle will be returned
        context: dict
            Workflow execution context containing datasets and Python variables
            state.

        Returns
        -------
        vizier.workflow.module.ModuleHandle
        """
        cmd = module.command
        if cmd.is_type(cmdtype.MODTYPE_PYTHON):
            cell = create_python_cell(module.identifier, cmd, context)
        elif cmd.is_type(cmdtype.MODTYPE_MIMIR):
            cell = create_mimir_cell(module.identifier, cmd, context)
        elif cmd.is_type(cmdtype.MODTYPE_VIZUAL):
            cell = create_vizual_cell(module.identifier, cmd, context)
        else:
            raise ValueError('unknown module type \'' + cmd.module_type + '\'')
        # Execute cell and get output
        try:
            cell.compute()
            outputs = cell.get_output('output')
        except Exception as ex:
            outputs = dict({TXT_NORMAL: list()})
            outputs[TXT_ERROR] = [str(ex)]
        # Return new module. Copies current state of the datastore mapping.
        return ModuleHandle(
            module.identifier,
            module.command,
            datasets=dict(
                ctx.get_datasets(
                    context[ctx.VZRENV_DATASETS],
                    module.identifier
                )
            ),
            stdout=outputs[TXT_NORMAL],
            stderr=outputs[TXT_ERROR]
        )

    def execute_workflow(self, version, modules, modified_index):
        """Execute a sequence of modules that define the next version of a given
        workflow in a viztrail. The list of modules is a modified list compared
        to the module in the given workflow. The modified_index points to the
        position (i.e., module) in the list of modules that contains the (first)
        modified module. All modules before modified_index remain the same as in
        the previous version of the workflow.

        The modified index may be negative. In that case execution starts at the
        first module.

        Parameters
        ----------
        version: int
            Unique version identifier for new workflow
        parent_version: int
            Version number of the parent workflow
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow versions
        modified_index: int
            Index position of the first modified module in modules

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        # Create global mapping of datasets for all exisitng modules.
        dataset_maps = list()
        for module in modules:
            dataset_maps.append({
                ctx.VZRENV_DATASETS_MODULEID: module.identifier,
                ctx.VZRENV_DATASETS_MAPPING: dict(module.datasets)
            })
        # New context for workflow execution
        context = WorkflowContext(self.exec_env, datasets=dataset_maps)
        # Set the index of the first module that may have changed outputs. Start
        # at the beginning of the workflow if modified_index is negative.
        if modified_index == -1:
            start_index = 0
        else:
            start_index = modified_index
        # List of executed modules for new workflow
        wf_modules = []
        # Flag indicating whether there has been an error during workflow
        # execution. All modules that are following a modules whose execution
        # failed are not executed.
        has_error = False
        # Iterate through the modules. Modules that occur before start_index are
        # assumed to have the same outputs as before. These modules do not need
        # to be executed again with the exception of PythonCells in order to
        # set global variables.
        for i in range(len(modules)):
            module = modules[i]
            if has_error:
                module = ModuleHandle(
                    module.identifier,
                    module.command,
                    stdout=list(),
                    stderr=list()
                )
            else:
                if i < start_index:
                    if module.command.is_type(cmdtype.MODTYPE_PYTHON):
                        # Save original module dataset mapping. This mapping
                        # should not change.
                        m_datasets = module.datasets
                        # Re-run the module to update the global state
                        module = self.execute_module(
                            module,
                            WorkflowContext(
                                self.exec_env,
                                context_type=ctx.CONTEXT_VOLATILE,
                                datasets=dataset_maps,
                                variables=context[ctx.VZRENV_VARS],
                            )
                        )
                        # Set module dataset mapping to original values
                        module.datasets = m_datasets
                    else:
                        # Copy the module
                        module = module.copy()
                else:
                    module = self.execute_module(module, context)
                has_error = module.has_error
            wf_modules.append(module)
        # Return handle for new workflow
        if start_index < len(wf_modules):
            mod_id = wf_modules[start_index].identifier
        else:
            mod_id = -1
        return WorkflowExecutionResult(version, mod_id, wf_modules)


class InputPort(object):
    """Simple implementation of input port for Vistrails modules."""
    def __init__(self, obj, spec=None, typecheck=None):
        # typecheck is a list of booleans indicating which descriptors to
        # typecheck
        self.obj = obj
        self.typecheck = None

    def clear(self):
        """clear() -> None. Removes references, prepares for deletion."""
        self.obj = None
        self.port = None

    def depth(self, fix_list=True):
        return 0

    def get_raw(self):
        """get_raw() -> Module. Returns the value or a Generator."""
        return self.obj


    def __call__(self):
        return self.get_raw()


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def create_mimir_cell(module_id, command, context):
    """Create a new Mimir cell module from the given command specification.

    Assumes that the validity of the command has been verified.

    Expected arguments:
    - name: Lens name
    - args: Lens-specific arguments
    - dataset: Dataset name

    Parameters
    ----------
    module_id: int
        Module identifier
    command: vizier.worktrail.module.ModuleSpecification
        Command specification
    context: dict
        Workflow execution context

    Returns
    -------
    vizier.packages.userpackages.vizierpkg.PythonCell
    """
    # Create a new python cell and set the input ports
    cell = MimirLens()
    cell.moduleInfo['moduleId'] = module_id
    cell.set_input_port('name', InputPort(command.command_identifier))
    cell.set_input_port('arguments', InputPort(command.arguments))
    cell.set_input_port('context', InputPort(context))
    return cell


def create_python_cell(module_id, command, context):
    """Create a new python cell module from the given command specification.

    Assumes that the validity of the command has been verified.

    Expected arguments:
    - source: Python source code for cell

    Parameters
    ----------
    module_id: int
        Module identifier
    command: vizier.worktrail.module.ModuleSpecification
        Command specification
    context: dict
        Workflow execution context

    Returns
    -------
    vizier.packages.userpackages.vizierpkg.PythonCell
    """
    # Create a new python cell and set the input ports
    cell = PythonCell()
    cell.moduleInfo['moduleId'] = module_id
    cell.set_input_port('source', InputPort(command.arguments['source']))
    cell.set_input_port('context', InputPort(context))
    return cell


def create_vizual_cell(module_id, command, context):
    """Create a new python cell module from the given command specification.

    Assumes that the validity of the command has been verified.

    Expected arguments:
    - source: Python source code for cell

    Parameters
    ----------
    module_id: int
        Module identifier
    command: vizier.worktrail.module.ModuleSpecification
        Command specification
    context: dict
        Workflow execution context

    Returns
    -------
    vizier.packages.userpackages.vizierpkg.PythonCell
    """
    # Create a new python cell and set the input ports
    cell = VizualCell()
    cell.moduleInfo['moduleId'] = module_id
    cell.set_input_port('name', InputPort(command.command_identifier))
    cell.set_input_port('arguments', InputPort(command.arguments))
    cell.set_input_port('context', InputPort(context))
    return cell
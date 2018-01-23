"""Simple implementation of the WorkflowEngine for testing purposes."""


import vizier.workflow.repository.command as cmd
from vizier.workflow.repository.engine.base import WorkflowExecutionResult, WorkflowEngine


"""Default test engine identifier."""
TEST_ENGINE_IDENTIFIER = 'TEST'


class TestWorkflowEngine(WorkflowEngine):
    """Simple test engine. Implements the abstract WorkflowEngine class. Does
    not execute any modules but simply sets the STDOUT to 'SUCCESS ,module-id>'.
    """
    def __init__(self, identifier=TEST_ENGINE_IDENTIFIER):
        """Set the engine identifier. The default identifier is TEST but in case
        multiple instances are being used each should have a unique identifier.

        Parameters
        ----------
        identifier: string
            Unique workflow engine identifier
        """
        super(TestWorkflowEngine, self).__init__(
            identifier,
            {
                cmd.MODTYPE_PYTHON: cmd.PYTHON_COMMANDS,
                cmd.MODTYPE_VIZUAL: cmd.VIZUAL_COMMANDS,
                cmd.MODTYPE_MIMIR: cmd.MIMIR_LENSES
            }
        )

    def copy_workflow(self, version_counter, modules):
        """Make a copy of the given workflow up until the given module
        (including). If module identifier is negative the complete workflow is
        copied.

        Parameters
        ----------
        version_counter: vizier.core.util.Sequence
            Unique version identifier generator
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        return WorkflowExecutionResult(
            version_counter.inc(),
            modules[0].identifier,
            [m.copy() for m in modules]
        )

    def execute_workflow(self, version_counter, module_counter, modules, modified_index):
        """Execute a sequence of modules that define the next version of a given
        workflow in a viztrail. The list of modules is a modified list compared
        to the module in the given workflow. The modified_index points to the
        position (i.e., module) in the list of modules that contains the (first)
        modified module. All modules before modified_index remain the same as in
        the previous version of the workflow.

        The modified index may be negative. In that case execution starts at the
        first module.

        The test engine sets the standard output of all modules that would be
        executes to 'SUCCESS <module-id>'. There is no actual execution of any
        modules.

        Parameters
        ----------
        version_counter: vizier.core.util.Sequence
            Unique version identifier generator
        module_counter: vizier.core.util.Sequence
            Unique module identifier generator
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
        version = version_counter.inc()
        if modified_index == -1:
            start_index = 0
        else:
            start_index = modified_index
        wf_modules = list()
        for i in range(len(modules)):
            m = modules[i].copy()
            if i >= start_index:
                if m.identifier < 0:
                    m.identifier = module_counter.inc()
                m.stdout.append('SUCCESS ' + str(m.identifier))
            elif  m.identifier < 0:
                raise ValueError('invalid module identifier \'' + str(m.identifier) + '\'')
            wf_modules.append(m)
        # Get the identifier of the modified module. The start_index may point
        # beyond the end of the modul elist if we deleted the last module. In
        # this case the result is -1
        if start_index < len(wf_modules):
            mod_id = wf_modules[start_index].identifier
        else:
            mod_id = -1
        return WorkflowExecutionResult(version, mod_id, wf_modules)

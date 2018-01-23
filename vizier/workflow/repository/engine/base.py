"""Vizier DB Workflow API - Workflow engine

Execute a workflow given as a sequence of workflow modules.
"""
from abc import abstractmethod

from vizier.core.timestamp import  get_current_time
from vizier.workflow.base import TXT_ERROR, TXT_NORMAL
from vizier.workflow.repository.command import MODULE_ARGUMENTS, validate_arguments


class WorkflowExecutionResult(object):
    """Result of executing a data curation workflow. Contains the version number
    of the executed workflow, the identifier of the modified module, and the
    list of modules together with their outputs.

    Note that the outputs of all modules that are located after to modified
    module may have changed.

    Attributes
    ----------
    module_id: int
        Identifier of modified module.
    modules: list(vizier.workflow.module.ModuleHandle)
        Sequence of mosules in the executed workflow
    version: int
        Version number pf the executed workflow
    """
    def __init__(self, version, module_id, modules):
        """Initialize the version number, module identifier, and list of modules
        that make up the result of workflow execution.

        Parameters
        ----------
        version: int
            Version number pf the executed workflow
        module_id: int
            Identifier of modified module.
        modules: list(vizier.workflow.module.ModuleHandle)
            Sequence of mosules in the executed workflow
        """
        self.version = version
        self.module_id = module_id
        self.modules = modules


class WorkflowEngine(object):
    """The workflow engine is used to execute workflows that are defined as
    sequences of modules.
    """
    def __init__(self, identifier, commands):
        """Initialize the engine identifier and the repository of supported
        commands.

        Parameters
        ----------
        identifier: string
            Unique engine identifier
        commands: dict
            Dictionary containing specifications for all commands that are
            supported by the engine. The dictionary structure is:
            module-type:
                command-identifier:
                    name: string
                    arguments: dict
        """
        self.identifier = identifier
        self.commands = commands

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def execute_workflow(self, version_counter, module_counter, modules, modified_index):
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
        raise NotImplementedError

    def validate_command(self, command):
        """Validate the given command specification. Raises ValueError if an
        invalid specification is given.

        Parameters
        ----------
        command : vizier.workflow.module.ModuleSpecification
            Specification of the command that is to be evaluated
        """
        cmd_type = command.module_type
        if not cmd_type in self.commands:
            raise ValueError('unknown command type \'' + cmd_type + '\'')
        type_commands = self.commands[cmd_type]
        command_id = command.command_identifier
        if not command_id in type_commands:
            raise ValueError('unknown command \'' + command_id + '\'')
        validate_arguments(
            type_commands[command_id][MODULE_ARGUMENTS],
            command.arguments
        )

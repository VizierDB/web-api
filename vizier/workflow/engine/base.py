# Copyright (C) 2018 New York University
#                    University at Buffalo,
#                    Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Vizier DB Workflow API - Workflow engine

Execute a workflow given as a sequence of workflow modules.
"""
from abc import abstractmethod

from vizier.core.timestamp import  get_current_time


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

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        modules: list(vizier.workflow.module.ModuleHandle)
            List of modules for the new workflow versions
        modified_index: int
            Index position of the first modified module in modules

        Returns
        -------
        vizier.workflow.engine.base.WorkflowExecutionResult
        """
        raise NotImplementedError

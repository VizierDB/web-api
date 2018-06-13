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

"""Vizier DB Workflow API - Viztrails.

Base classes for workflows and viztrails. Workflow are sequences of modules.
Viztrails are collections of workflows (branches). A viztrail maintains not
only the different workflows but also the history for each of them.
"""

from vizier.core.timestamp import get_current_time, to_datetime

import vizier.workflow.command as cmd


"""Identifier of the default master branch for all viztrails."""
DEFAULT_BRANCH = 'master'

"""Default name for the master branch."""
DEFAULT_BRANCH_NAME = 'Default'

"""Workflow modification action identifier."""
ACTION_CREATE = 'cre'
ACTION_DELETE = 'del'
ACTION_INSERT = 'ins'
ACTION_REPLACE = 'upd'


class WorkflowVersionDescriptor(object):
    """Simple workflow descriptor that contains the workflow version and the
    time of creation.

    Attributes
    ----------
    version: int
        Workflow version identifier
    create_at: datetime.datetime, optional
        Timestamp of workflow creation (UTC)
    """
    def __init__(self, version, action=None, package_id=None, command_id=None, created_at=None):
        """Initialize the descriptor.

        Parameters
        ----------
        version: int
            Workflow version identifier
        actions: string
            Identifier of the action that created the workflow version (create,
            insert, delete, or replace)
        package_id: string
            Identifier of the package the module command is from
        command_id: string
            Identifier of the module command
        create_at: datetime.datetime
            Timestamp of workflow creation (UTC)
        """
        self.version = version
        self.action = action
        self.package_id = package_id
        self.command_id = command_id
        self.created_at = created_at if not created_at is None else get_current_time()

    @staticmethod
    def from_dict(obj):
        """Create descriptor instance from dictionary serialization.

        Returns
        -------
        vizier.workflow.base.WorkflowVersionDescriptor
        """
        return WorkflowVersionDescriptor(
            obj['version'],
            action=obj['action'] if 'action' in obj else None,
            package_id=obj['packageId'] if 'packageId' in obj else None,
            command_id=obj['commandId'] if 'commandId' in obj else None,
            created_at=to_datetime(obj['createdAt'])
        )

    def to_dict(self):
        """Create dictionary serialization for the object.

        Returns
        -------
        dict
        """
        return {
            'version': self.version,
            'action': self.action,
            'packageId': self.package_id,
            'commandId': self.command_id,
            'createdAt': self.created_at.isoformat()
        }


class WorkflowHandle(object):
    """Handle for a data curation workflow. Workflows are sequences of modules
    that contain (i) the command specification, and (ii) outputs for STDOUT and
    STDERR generated during module execution. Each workflow belongs to a branch
    in the viztrail. Workflows have unique version numbers.

    Attributes
    ----------
    branch_id: string
        Unique identifier of the branch the workflow is associated with
    version: int
        Unique version number for the workflow
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    modules: list(vizier.workflow.module.ModuleHandle)
        Sequence of modules that make up the workflow and result in the current
        state of the database after successful workflow execution.
    """
    def __init__(self, branch_id, version, created_at, modules):
        """Initialize the workflow handle.

        Parameters
        ----------
        branch_id: string
            Unique identifier of the branch the workflow is associated with
        version : int
            Unique version number for the workflow
        created_at : datetime.datetime
            Timestamp of viztrail creation (UTC)
        modules : list(ModuleHandle)
            Sequence of modules that make up the workflow.
        """
        self.branch_id = branch_id
        self.version = version
        self.created_at = created_at
        self.modules = modules

    @property
    def has_error(self):
        """Flag indicating whether there was an error during workflow execution.
        Currently, only the existience of output to STDERR in at least one of
        the modules is used as error indicator.

        Returns
        -------
        bool
        """
        for m in self.modules:
            if m.has_error:
                return True
        return False


class ViztrailBranch(object):
    """Branch in a viztrail. Each branch has a unique identifier, a set of user-
    defined properties, and a list of workflow versions that make up the history
    of the branch. The last entry in the workflow verion list references the
    current workflow (state) of the branch.

    Attributes
    ----------
    identifier: string
        Unique branch identifier
    properties: vizier.core.properties.ObjectPropertiesHandler
        Handler for user-defined properties that are associated with this
        viztrail branch
    provenance: vizier.workflow.base.ViztrailBranchProvenance
        Provenance information for this branch
    workflows: list(vizier.workflow.base.WorkflowVersionDescriptor)
        List of workflow versions that define the history of this branch
    """
    def __init__(self, identifier, properties, provenance, workflows=None):
        """Initialize the viztrail branch.

        identifier: string
            Unique branch identifier
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with the
            branch
        provenance: vizier.workflow.base.ViztrailBranchProvenance
            Provenance information for this branch
        versions: list(int), optional
            List of unique workflow versions that define the history of this
            branch. The last entry in the list references the current workflow
            of the branch
        """
        self.identifier = identifier
        self.properties = properties
        self.provenance = provenance
        self.workflows = workflows if not workflows is None else list()


class ViztrailBranchProvenance(object):
    """Object containing the source branch, workflow version, and module
    identifier from which a branch was created.

    Attributes
    ----------
    source_branch : string
        Unique branch identifier of source branch
    workflow_version: int
        Version number of source workflow.
    module_id: int
        Identifier of module at which the new branch started. Note that this
        identifier defines the last module in the workflow of the created
        branch.
    """
    def __init__(self, source_branch, workflow_version, module_id):
        """Initialize the provenance object.

        Parameters
        ----------
        source_branch : string
            Unique branch identifier of source branch
        workflow_version: int
            Version number of source workflow.
        module_id: int
            Identifier of module at which the new branch started
        """
        self.source_branch = source_branch
        self.workflow_version = workflow_version
        self.module_id = module_id


class ViztrailHandle(object):
    """Handle for a Vizier viztrail. Each viztrail has a unique identifier and
    a dictionary of branches keyed by their name. Branches are sequence of
    workflow version identifiers representing the history of the workflow for
    the respective branch.

    Attributes
    ----------
    identifier : string
        Unique viztrail identifier
    branches : dict(ViztrailBranch)
        Dictionary of branches. Each branch is represented by the sequence
        of workflow versions that represent the history of the workflow for
        the branch.
    env_id: string
        Unique execution environment identifier
    command_repository: dict
        Dictionary containing specifications for all commands that are
        supported by the execution environment. The dictionary structure is:
        module-type:
            command-identifier:
                name: string
                arguments: dict
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    last_modified_at : datetime.datetime
        Timestamp when viztrail was last modified (UTC)
    properties: vizier.core.properties.ObjectPropertiesHandler
        Handler for user-defined properties that are associated with this
        viztrail
    """
    def __init__(self, identifier, branches, env_id, command_repository, properties, created_at=None, last_modified_at=None):
        """Initialize the viztrail identifier and branch dictionary.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        branches : dict(ViztrailBranch)
            Dictionary of branches.
        env_id: string
            Unique execution environment identifier
        command_repository: dict
            Dictionary containing specifications for all commands that are
            supported by the execution environment.
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with this
            viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        last_modified_at : datetime.datetime, optional
            Timestamp when project was last modified (UTC)
        """
        self.identifier = identifier
        self.branches = branches
        self.env_id = env_id
        self.command_repository = command_repository
        self.properties = properties
        # If created_at timestamp is None the viztrail is expected to be a newly
        # created viztrail. For new viztrails the last_modified timestamp and
        # branches listing are expected to be None. For existing viztrails
        # last_modified and branches should not be None
        if not created_at is None:
            if last_modified_at is None:
                raise ValueError('unexpected value for \'last_modified\'')
            self.created_at = created_at
            self.last_modified_at = last_modified_at
        else:
            if not last_modified_at is None:
                raise ValueError('missing value for \'last_modified\'')
            self.created_at = get_current_time()
            self.last_modified_at = self.created_at

    def validate_command(self, command):
        """Validate the given command specification. Raises ValueError if an
        invalid specification is given.

        Parameters
        ----------
        command : vizier.workflow.module.ModuleSpecification
            Specification of the command that is to be evaluated
        """
        cmd.validate_command(self.command_repository, command)

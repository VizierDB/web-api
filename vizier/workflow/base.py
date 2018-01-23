"""Vizier DB Workflow API - Viztrails.

Base classes for workflows and viztrails. Workflow are sequences of modules.
Viztrails are collections of workflows (branches). A viztrail maintains not
only the different workflows but also the history for each of them.
"""

from vizier.core.timestamp import get_current_time


"""Identifier of the default master branch for all viztrails."""
DEFAULT_BRANCH = 'master'

"""Default name for the master branch."""
DEFAULT_BRANCH_NAME = 'Default'

"""Constants to identify the different module output streams."""
TXT_ERROR = 'err'
TXT_NORMAL = 'out'


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
    versions: list(int)
        List of identifier of workflow versions that define the history of this
        branch
    """
    def __init__(self, identifier, properties, provenance, versions=None):
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
        self.versions = versions if not versions is None else list()


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
    engine_id: string
        Unique engine identifier
    command_repository: dict
        Dictionary containing specifications for all commands that are
        supported by the engine. The dictionary structure is:
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
    def __init__(self, identifier, branches, engine_id, command_repository, properties, created_at=None, last_modified_at=None):
        """Initialize the viztrail identifier and branch dictionary.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        branches : dict(ViztrailBranch)
            Dictionary of branches.
        engine_id: string
            Unique engine identifier
        command_repository: dict
            Dictionary containing specifications for all commands that are
            supported by the engine.
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
        self.engine_id = engine_id
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
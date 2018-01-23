"""Vizier DB Workflow API - Viztrail Repository

Specification of the viztrail repository that manages viztrails and the
execution of viztrail workflows.
"""

from abc import abstractmethod

from vizier.core.system import VizierSystemComponent
from vizier.workflow.base import DEFAULT_BRANCH


class ViztrailRepository(VizierSystemComponent):
    """Repository for viztrails. This is an abstract class that defines all
    necessary methods to maintain viztrails and to manipulate and execute
    workflows.

    By default all methods operate on the workflow that is at the HEAD of the
    defautl branch (if applicable).
    """
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(ViztrailRepository, self).__init__(build)

    @abstractmethod
    def append_workflow_module(self, viztrail_id=None, branch_id=DEFAULT_BRANCH, workflow_version=-1, command=None, before_id=-1):
        """Append a module to a workflow in a given viztrail. The module is
        appended to the workflow that is identified by the given version number.
        If the version number is negative the workflow at the branch HEAD is the
        one that is being modified.

        The modified workflow will be executed. The result is the new head of
        the branch.

        If before_id is non-negative the new module is inserted into the
        existing workflow before the module with the specified identifier. If no
        module with the given identifier exists a ValueError is raised. If
        before_id is negative, the new module is appended to the end of the
        workflow.

        Returns a handle to the state of the executed workflow. Returns None if
        the specified viztrail, branch, or workflow do not exist.

        Raises a ValueError if an invalid command specification is given.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        command : vizier.workflow.module.ModuleSpecification, optional
            Specification of the command that is to be evaluated
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def create_branch(self, viztrail_id=None, source_branch=DEFAULT_BRANCH, workflow_version=-1, module_id=-1, properties=None):
        """Create a new workflow branch in a given viztrail. The new branch is
        created from the specified workflow in the source branch starting at
        module module_id. If module_id is negative the new branch starts after
        the last module of the source branch head workflow.

        Returns the handle for the new branch or None if the given viztrail does
        not exist. Raises ValueError if (1) the source branch does not exist,
        (2) no module with the specified identifier exists, or (3) an attempt is
        made to branch from an empty workflow.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        source_branch : string, optional
            Unique branch identifier for existing branch
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        module_id: int, optional
            Start branch from module with given identifier in source_branch.
            The new branch starts at the end of the source branch if module_id
            has a negative value.
        properties: dict, optional
            Set of properties for the new branch

        Returns
        -------
        vizier.workflow.base.ViztrailBranch
        """
        raise NotImplementedError

    @abstractmethod
    def create_viztrail(self, engine_id, properties):
        """Create a new viztrail.

        Raises ValueError if the given workflow engine is unknown.

        Parameters
        ----------
        engine_id: string
            Identifier for workflow engine that is used to execute workflows of
            the new viztrail
        properties: dict
            Set of properties for the new viztrail

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_branch(self, viztrail_id=None, branch_id=None):
        """Delete the viztrail branch with the given identifier. Returns the
        modified viztrail handle. The result is None if either the branch or the
        viztrail is unknown.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        branch_id: string, optional
            Unique workflow branch identifier

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_workflow_module(self, viztrail_id=None, branch_id=DEFAULT_BRANCH, workflow_version=-1, module_id=-1):
        """Delete the module with the given identifier in the specified
        workflow. The resulting workflow is execute and the resulting workflow
        will form the new head of the given viztrail branch.

        The result is True on success. Returns False if no viztrail, branch, or
        module with given identifier exists.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        branch_id: string, optional
            Unique workflow branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        module_id : int, optional
            Module identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def delete_viztrail(self, viztrail_id=None):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_viztrail(self, viztrail_id=None):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, viztrail_id=None, branch_id=DEFAULT_BRANCH, workflow_version=-1):
        """Retrieve the workflow at the HEAD of the branch with branch_id in the
        given viztrail. The result is None if the viztrail or branch do not
        exist.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being retrieved. If negative
            the branch head is returned.

        Returns
        -------
        vizier.workflow.base.WorkflowHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.workflow.base.ViztrailHandle)
            List of viztrail handles
        """
        raise NotImplementedError

    @abstractmethod
    def replace_workflow_module(self, viztrail_id=None, branch_id=DEFAULT_BRANCH, workflow_version=-1, module_id=-1, command=None):
        """Replace an existing module in a workflow. The module is replaced in
        the workflow that is identified by the given version number. If the
        version number is negative the workflow at the branch HEAD is the
        one that is being modified. The modified workflow is executed and the
        result will be the new head of the branch.

        Returns a handle to the state of the executed workflow. Returns None if
        the specified viztrail, branch, workflow, or module do not exist.

        Raises a ValueError if an invalid command specification is given.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being retrieved. If negative
            the branch head is returned.
        module_id : int, optional
            Identifier of the module that is being replaced
        command : vizier.workflow.module.ModuleSpecification
            Specification of the command that is to be evaluated

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

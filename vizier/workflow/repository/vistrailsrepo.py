"""Implementation of a Viztrail repository using Vistrails as the underlying
workflow engine.
"""
import logging
import os
import shutil
import yaml

#import vistrails.core.db.io
#from vistrails.core.system import vistrails_version
from vistrails.core.db.locator import BaseLocator, UntitledLocator
from vistrails.core.vistrail.controller import VistrailController
import vistrails.core.application
#from vistrails.core.db.action import create_action
#from vistrails.core.modules.module_registry import get_module_registry
#from vistrails.core.modules.vistrails_module import ModuleError
from vistrails.core.packagemanager import get_package_manager

from vizier.core.properties import FilePropertiesHandler
from vizier.core.system import build_info, component_descriptor
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.core.util import get_unique_identifier
from vizier.workflow.base import ViztrailBranch, ViztrailHandle
from vizier.workflow.base import DEFAULT_BRANCH, DEFAULT_BRANCH_NAME
from vizier.workflow.repository.base import ViztrailRepository
from vizier.workflow.repository.fs import FileSystemBranchProvenance
from vizier.workflow.repository.fs import branch_file, branch_prov_file


"""Files containing viztrail information."""
PROPERTIES_FILE = 'properties.yaml'
VISTRAIL_FILE = 'vistrail.vt'
VIZTRAIL_FILE = 'viztrail.yaml'


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
        'userPackageDir': os.path.join(os.getcwd(), 'workflow/packages/userpackages'),
        # Set dotVistrailes for local server
        'dotVistrails': '../.env/.vt',
    },
    args=[])

get_package_manager().late_enable_package('vizierpkg')


# ------------------------------------------------------------------------------
# Classes
# ------------------------------------------------------------------------------

class VistrailWrapper(ViztrailHandle):
    """Wrapper around aVistrail workflow file. Maintains additional metadata in
    a Yaml file.

    All information about a viztrail is stored in a separate directory. The
    following files will be created:

    - viztrail.yaml: Viztrail state
    - properties.yaml: Viztrail properties
    - <branch-id>_properties.yaml: For each branch a properties file that is
      prefixed by the branch identifier is created. The branch information
      itself (i.e., list of workflow versions in the branch) is maintained as
      part of the viztrail state
    - <branch-id>_provanence.yaml: For each branch provenance information is
      kept in a file prefixed by the branch identifier.
    - vistrail.vt: Vistrail file maintaining different workflow versions.
    """
    def __init__(self, identifier, branches, engine, properties, controller, created_at=None, last_modified_at=None, fs_dir=None):
        """Initialize the viztrail handle. Raise a ValueError exception if no
        base directory is given.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        branches : dict(ViztrailBranch)
            Dictionary of branches.
        engine: vizier.workflow.engine.base.WorkflowEngine
            Workflow engine for execution of viztrail workflows
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with this
            viztrail
        controller: vistrails.core.vistrail.controller.VistrailController
            Controller for the associated VisTrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        last_modified_at : datetime.datetime, optional
            Timestamp when project was last modified (UTC)
        fs_dir: string
            Base directory where all viztrail information is stored
        """
        super(VistrailWrapper, self).__init__(
            identifier,
            branches,
            engine.identifier,
            engine.commands,
            properties,
            created_at=created_at,
            last_modified_at=last_modified_at
        )
        self.engine = engine
        self.controller = controller
        # Ensure that the base directory is not None
        if fs_dir is None:
            raise ValueError('missing base directory for viztrail')
        self.fs_dir = fs_dir

    @staticmethod
    def create_viztrail(fs_dir, identifier, engine, properties=None):
        """Create a new viztrail handle.

        Parameters
        ----------
        fs_dir: string
            Base directory where all viztrail information is stored
        identifier: string
            Unique viztrail identifier
        engine: vizier.workflow.engine.base.WorkflowEngine
            Workflow engine for execution of viztrail workflows
        properties: dict, optional
            Optional dictionary of viztrail properties

        Returns
        -------
        vizier.workflow.repository.fs.FileSystemViztrailHandle
        """
        # Create a new Vistrail
        locator = UntitledLocator()
        loaded_objs = vistrails.core.db.io.load_vistrail(locator)
        controller = VistrailController(
            loaded_objs[0],
            locator,
            *loaded_objs[1:]
        )
        locator = BaseLocator.from_url(os.path.join(fs_dir, VISTRAIL_FILE))
        controller.flush_delayed_actions()
        controller.write_vistrail(locator)
        # Create the master branch
        master_branch = ViztrailBranch(
            DEFAULT_BRANCH,
            FilePropertiesHandler(
                branch_file(fs_dir, DEFAULT_BRANCH),
                properties={'name': DEFAULT_BRANCH_NAME}
            ),
            FileSystemBranchProvenance(branch_prov_file(fs_dir, DEFAULT_BRANCH))
        )
        # Create viztrail and write to file system
        viztrail = VistrailWrapper(
            identifier,
            {DEFAULT_BRANCH: master_branch},
            engine,
            FilePropertiesHandler(
                os.path.join(fs_dir, PROPERTIES_FILE),
                properties=properties
            ),
            controller,
            fs_dir=fs_dir
        )
        viztrail.to_file()
        # Return the new viztrail handle
        return viztrail

    def delete(self):
        """Delete the viztrail by removing the base directory."""
        shutil.rmtree(self.fs_dir)

    @staticmethod
    def from_file(fs_dir, engines):
        """Read the viztrail state from file.

        Raises IOError if the viztrail file does not exist.

        Parameters
        ----------
        fs_dir: string
            Base directory where all viztrail information is stored
        engines: dict(string: vizier.workflow.engine.base.WorkflowEngine)
            Dictionary of workflow engines

        Returns
        -------
        vizier.workflow.repository.fs.FileSystemViztrailHandle
        """
        # Read vizrail information for file (in Yaml format)
        with open(os.path.join(fs_dir, VIZTRAIL_FILE), 'r') as f:
            doc = yaml.load(f.read())
        # Get the controller for the associated Vistrail
        path = os.path.join(fs_dir, VISTRAIL_FILE)
        locator = BaseLocator.from_url(path)
        loaded_objs = vistrails.core.db.io.load_vistrail(locator)
        controller = VistrailController(
            loaded_objs[0],
            locator,
            *loaded_objs[1:]
        )
        # Read information about viztrail branches
        branches = {
            b['id'] : ViztrailBranch(
                b['id'],
                FilePropertiesHandler(branch_file(fs_dir, b['id'])),
                FileSystemBranchProvenance(branch_prov_file(fs_dir, b['id'])),
                versions=b['versions']
            )  for b in doc['branches']
        }
        # Return vistrails wrapper
        return VistrailWrapper(
            doc['id'],
            branches,
            engines[doc['engine']],
            FilePropertiesHandler(os.path.join(fs_dir, PROPERTIES_FILE)),
            controller,
            created_at=to_datetime(doc['timestamps']['createdAt']),
            last_modified_at=to_datetime(doc['timestamps']['lastModifiedAt']),
            fs_dir=fs_dir
        )

    def to_file(self):
        """Write the current state of the viztrail to file. Sets the last
        modified at timestamp to the current time.
        """
        self.last_modified_at = get_current_time()
        # Serialize viztrail
        doc = {
            'id': self.identifier,
            'engine': self.engine.identifier,
            'branches' : [{
                    'id': b,
                    'versions': self.branches[b].versions
                } for b in self.branches
            ],
            'timestamps' : {
                'createdAt' : self.created_at.isoformat(),
                'lastModifiedAt' : self.last_modified_at.isoformat()
            }
        }
        # Write viztrail serialization to file
        with open(os.path.join(self.fs_dir, VIZTRAIL_FILE), 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


class VistrailRepository(ViztrailRepository):
    """Repository for viztrails implemented using Vistrails as the underlying
    workflow engine.
    """
    def __init__(self, base_directory, engines):
        """Initialize the base directory and the dictionary of workflow engines.

        The base directory is created if it does not exist. Viztrail information
        is maintained in subfolders of the base directory. For each viztrail one
        subfolder is created. There shold be no other folders in the base
        directory other than those containing information about a viztrail.

        Parameters
        ---------
        base_directory : string
            Path to base directory
        engines: dict(string: vizier.workflow.engine.base.WorkflowEngine)
            Dictionary of available workflow engines
        """
        super(VistrailRepository, self).__init__(
            build_info('VistrailRepository')
        )
        self.base_dir = os.path.abspath(base_directory)
        # Create base directory if it doesn't exist
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)
        # Set list of workflow engines
        self.engines = engines
        # Read information about all available viztrails into an internal cache.
        # The cache is a dictionary of viztrail handles (keyes by their
        # identifier). Assumes that every directory in the base dir represents
        # a viztrail
        self.cache = dict()
        for filename in os.listdir(self.base_dir):
            fs_dir = os.path.join(self.base_dir, filename)
            if os.path.isdir(fs_dir):
                viztrail = VistrailWrapper.from_file(
                    fs_dir,
                    self.engines
                )
                self.cache[viztrail.identifier] = viztrail

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

    def components(self):
        """List containing component descriptor and descriptors for workflow
        engine component.

        Returns
        -------
        list
        """
        return [component_descriptor('viztrails', self.system_build())]

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
        if not engine_id in self.engines:
            raise ValueError('unknown workflow engine \'' + engine_id + '\'')
        # Get unique viztrail identifier
        identifier = get_unique_identifier()
        # Create viztrail directory
        fs_dir = os.path.join(self.base_dir, identifier)
        os.makedirs(fs_dir)
        # Create new viztrail and add to cache
        viztrail = VistrailWrapper.create_viztrail(
            fs_dir,
            identifier,
            self.engines[engine_id],
            properties=properties
        )
        self.cache[viztrail.identifier] = viztrail
        return viztrail

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
        # Raise exception if branch is the default branch
        if branch_id == DEFAULT_BRANCH:
            raise ValueError('attempt to delete default viztrail branch')
        # Get viztrail. Return None if it doen't exist
        if not viztrail_id in self.cache:
            return None
        viztrail = self.cache[viztrail_id]
        # Get viztrail branch. Return None if branch does not exist
        if not branch_id in viztrail.branches:
            return None
        branch = viztrail.branches[branch_id]
        # Delete branch properties file
        os.remove(branch_file(viztrail.fs_dir, branch_id))
        # Update the viztrail information
        del viztrail.branches[branch_id]
        viztrail.to_file()
        return viztrail

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
        if viztrail_id in self.cache:
            # Delete viztrail directory if the viztrail exists
            self.cache[viztrail_id].delete()
            del self.cache[viztrail_id]
            return True
        else:
            return False

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
        # Return information directly from the cache
        return self.cache[viztrail_id] if viztrail_id in self.cache else None

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

    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.workflow.base.ViztrailHandle)
            List of viztrail handles
        """
        # Return list of values in cache
        return self.cache.values()

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

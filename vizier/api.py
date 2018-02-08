"""Vizier DB Web API - Implements the methods of the Web API that correspond
to valid Http requests for the Vizier Web Service.

The separate API module separates the implementation from the specifics of the
Web server (e.g., Flask or Tornado). The module orchestrates the interplay
between different components such as the notbook repository that manages
noretbook metadata and the VizTrails module.
"""

import gzip
import os

from vizier.hateoas import UrlFactory, reference, self_reference
from vizier.workflow.base import DEFAULT_BRANCH

import vizier.workflow.command as cmd


"""Frequently used serialization element labels."""
JSON_REFERENCES = 'links'


"""HATEOAS relation identifier."""
REL_ANNOTATED = 'annotated'
REL_ANNOTATIONS='annotations'
REL_APIDOC = 'doc';
REL_APPEND = 'append';
REL_BRANCH = 'branch'
REL_BRANCHES = 'branches'
REL_CREATE = 'create'
REL_CURRENT_VERSION = 'currentVersion'
REL_DATASET = 'dataset'
REL_DELETE = 'delete'
REL_DOWNLOAD = 'download'
REL_FILES = 'files'
REL_HEAD = 'head'
REL_INSERT = 'insert'
REL_MODULES = 'modules'
REL_MODULE_SPECS = 'environment'
REL_NOTEBOOK = 'notebook'
REL_PROJECT = 'project'
REL_PROJECTS = 'projects'
REL_RENAME = 'rename'
REL_REPLACE = 'replace'
REL_SERVICE = 'home'
REL_SYSTEM_BUILD = 'build'
REL_UPDATE = 'update'
REL_UPLOAD = 'upload'
REL_WORKFLOW = 'workflow'


class VizierWebService(object):
    """The Web Service API implements the methods that correspond to the Http
    requests that are handled by the Web server.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service (i.e., viztrail repository, data
    store, and file server).
    """
    def __init__(self, viztrail_repository, datastore, fileserver, config):
        """Initialize the API from a dictionary. Expects the following keys to
        be present in the dictionary:
        - APP_NAME : Application (short) name for the service description
        - API_DOC : Url for API documentation
        - SERVER_APP_PATH : Application path part of the Url to access the app
        - SERVER_URL : Base Url of the server where the app is running
        - SERVER_PORT : Port the server is running on

        Parameters
        ----------
        viztrail_repository : workflow.ViztrailRepository
            Repository for viztrails (aka projects)
        datastore : database.DataStore
            Backend store for datasets
        fileserver: database.fileserver.FileServer
            Backend store for uploaded CSV files
        config : vizier.config.AppConfig
            Application configuration parameters
        """
        self.viztrails = viztrail_repository
        self.datastore = datastore
        self.fileserver = fileserver
        # Initialize the factory for API resource Urls
        self.urls = UrlFactory(config)
        # Initialize the service description dictionary
        self.service_descriptor = {
            'name' : config.name,
            'envs': [{
                    'id': config.envs[key].identifier,
                    'name': config.envs[key].name,
                    'description': config.envs[key].description,
                    'default': config.envs[key].default
                } for key in config.envs
            ],
            JSON_REFERENCES : [
                self_reference(self.urls.service_url()),
                reference(REL_SYSTEM_BUILD, self.urls.system_build_url()),
                reference(REL_PROJECTS, self.urls.projects_url()),
                reference(REL_FILES, self.urls.files_url()),
                reference(REL_UPLOAD, self.urls.files_upload_url()),
                reference(REL_APIDOC, config.api.doc_url)
            ]
        }

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------
    def service_overview(self):
        """Returns a dictionary containing essential information about the web
        service including HATEOAS links to access resources and interact with
        the service.

        Returns
        -------
        dict
        """
        return self.service_descriptor

    def system_build(self):
        """Returns a dictionary with information about individual system
        components, including version information for software components.

        Returns
        -------
        dict
        """
        components = list()
        components.extend(self.datastore.components())
        components.extend(self.fileserver.components())
        components.extend(self.viztrails.components())
        return {
            'components' : components,
            JSON_REFERENCES : [
                self_reference(self.urls.system_build_url()),
                reference(REL_SERVICE, self.urls.service_url())
            ]
        }

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def delete_file(self, file_id):
        """Delete a file from the file server.

        Parameters
        ----------
        file_id: string
            Unique file identifier

        Returns
        -------
        bool
        """
        return self.fileserver.delete_file(file_id)

    def get_file(self, file_id):
        """Get handle for file resource on file server. Returns None if no file
        with the given identifier exists.

        Parameters
        ----------
        file_id: string
            Unique file identifier

        Returns
        -------
        dict
        """
        f_handle = self.fileserver.get_file(file_id)
        if not f_handle is None:
            return self.serialize_file_handle(f_handle)
        return None

    def get_file_reader(self, file_id):
        """Get the content of a CSV file as a list of rows. Returns a tuple of
        rows and the file base name.

        Parameters
        ----------
        file_id: string
            Unique file identifier

        Returns
        -------
        list(csv.row), string
        """
        f_handle = self.fileserver.get_file(file_id)
        if not f_handle is None:
            rows = list()
            csvfile = self.fileserver.open_file(file_id)
            for row in csvfile.reader:
                rows.append(row)
            csvfile.close()
            return rows, f_handle.base_name
        return None, None

    def list_files(self):
        """Get list of file resources currently available from the file server.

        Returns
        -------
        dict
        """
        return {
            'files': [
                self.serialize_file_handle(f)
                    for f in self.fileserver.list_files()
            ],
            JSON_REFERENCES : [
                self_reference(self.urls.files_url()),
                reference(
                    REL_UPLOAD,
                    self.urls.files_upload_url()
                )
            ]
        }

    def rename_file(self, file_id, name):
        """Rename file with given identifier. Raises ValueError if a file with
        the given name already exists.

        Parameters
        ----------
        file_id: string
            Unique file identifier
        name: string
            New file name

        Returns
        -------
        dict
        """
        f_handle = self.fileserver.rename_file(file_id, name)
        if not f_handle is None:
            return self.serialize_file_handle(f_handle)
        return None

    def serialize_file_handle(self, f_handle):
        """Create dictionary serialization for dataset instance.

        Parameters
        ----------
        f_handle : database.fileserver.FileHandle
            Handle for file server resource

        Returns
        -------
        dict
        """
        self_ref = self.urls.file_url(f_handle.identifier)
        return {
            'id': f_handle.identifier,
            'name' : f_handle.name,
            'columns' : f_handle.columns,
            'rows': f_handle.rows,
            'createdAt': f_handle.created_at.isoformat(),
            JSON_REFERENCES : [
                self_reference(self_ref),
                reference(REL_DELETE, self_ref),
                reference(REL_RENAME, self_ref),
                reference(
                    REL_DOWNLOAD,
                    self.urls.file_download_url(f_handle.identifier)
                )
            ]
        }

    def upload_file(self, filename):
        """Upload a given file to the file server. Expects either a CSV or TSV
        file. The file type is determined by the file suffix.

        Parameters
        ----------
        filename : string
            path to file on local disk

        Returns
        -------
        dict
            Dictionary for dataset descriptor
        """
        # Parse file and add to datastore
        f_handle = self.fileserver.upload_file(filename)
        return self.serialize_file_handle(f_handle)


    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, dataset_id, include_annotations=False):
        """Get dataset with given identifier. The result is None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        include_annotations: bool
            Flag indicating whether dataset annotations should be included in
            the result

        Returns
        -------
        dict
            Dictionary representation for dataset state
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.datastore.get_dataset(dataset_id)
        if not dataset is None:
            # Serialize the dataset schema and cells
            obj = {
                'id' : dataset.identifier,
                'uri' : 'dataset://' + dataset.identifier,
                'columns' : [
                    {'id': col.identifier, 'name': col.name}
                        for col in dataset.columns
                ],
                'rows': [
                    {'id': row.identifier, 'values': row.values}
                        for row in dataset.rows
                ]
            }
            if include_annotations:
                obj['annotations'] = self.serialize_dataset_annotations(
                    dataset_id,
                    dataset.annotations
                )
            # Add references if dataset exists
            obj[JSON_REFERENCES] = [
                self_reference(self.urls.dataset_url(dataset_id)),
                reference(
                    REL_DOWNLOAD,
                    self.urls.dataset_download_url(dataset_id)
                ),
                reference(
                    REL_ANNOTATIONS,
                    self.urls.dataset_annotations_url(dataset_id)
                )
            ]
            return obj
        else:
            return None

    def get_dataset_annotations(self, dataset_id):
        """Get annotations for dataset with given identifier. The result is None
        if no dataset with the given identifier exists.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        dict
            Dictionary representation for dataset annotations
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        dataset = self.datastore.get_dataset(dataset_id)
        if not dataset is None:
            return self.serialize_dataset_annotations(
                dataset_id,
                dataset.annotations
            )
        else:
            return None

    def serialize_dataset_annotations(self, dataset_id, annotations):
        """Get dictionary serialization for dataset annotations.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        annotations: vizier.datastore.metadata.DatasetMetadata
            Set of annotations for dataset components

        Returns
        -------
        dict
        """
        obj = annotations.to_dict()
        # Add references if dataset exists
        obj[JSON_REFERENCES] = [
            self_reference(self.urls.dataset_annotations_url(dataset_id)),
            reference(
                REL_DATASET,
                self.urls.dataset_url(dataset_id)
            )
        ]
        return obj

    def update_dataset_annotation(self, dataset_id, upd_statement):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the modified object annotations or None if the
        dataset does not exist.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        upd_statement: vizier.datastore.metadata.AnnotationUpdateStatement
            Update statement that handles update of an existing DatasetMetadata
            object.

        Returns
        -------
        dict
        """
        # Get dataset with given identifier from data store. If the dataset
        # does not exist the result is None.
        annotations = self.datastore.update_annotation(dataset_id, upd_statement)
        if not annotations is None:
            return self.serialize_dataset_annotations(dataset_id, annotations)
        else:
            return None


    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self, env_id, properties):
        """Create a new project. All the information about a project is
        currently stored as part of the viztrail.

        Parameters
        ----------
        env_id: string
            Unique identifier of the execution environment for the new viztrail
        properties : dict
            Dictionary of user-defined project properties

        Returns
        -------
        dict
            Handle for new project
        """
        # Create a new viztrail.
        viztrail = self.viztrails.create_viztrail(env_id, properties)
        # Return a serialization of the new project.
        return self.serialize_project_handle(viztrail)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes the Vistrails
        workflow that is associated with the project and the entry in the
        repository. Both are identified by the same id.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
            True, if project existed and False otherwise
        """
        # Delete entry in repository. The result indicates whether the project/
        # viztrail existed or not.
        return self.viztrails.delete_viztrail(viztrail_id=project_id)

    def get_project(self, project_id):
        """Get comprehensive information for the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        dict
            Serialization of the project handle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Get serialization for project handle.
        return self.serialize_project_handle(viztrail)

    def list_module_specifications_for_project(self, project_id):
        """Retrieve list of parameter specifications for all supported modules
        for the given project. Returns None if no project (viztrail) with
        given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        modules = []
        for module_type in viztrail.command_repository:
            type_commands = viztrail.command_repository[module_type]
            for command_id in type_commands:
                arguments = type_commands[command_id][cmd.MODULE_ARGUMENTS]
                modules.append({
                    'type': module_type,
                    'id': command_id,
                    'name': type_commands[command_id][cmd.MODULE_NAME],
                    'arguments': [arguments[arg] for arg in arguments]
                })
        return {
            'project': self.serialize_project_descriptor(viztrail),
            'modules': modules,
            JSON_REFERENCES : [
                self_reference(
                    self.urls.project_module_specs_url(viztrail.identifier)
                ),
            ]
        }

    def list_projects(self):
        """Returns a list of descriptors for all projects that are currently
        contained in the project repository.

        Returns
        ------
        dict
        """
        return {
            'projects' : [
                self.serialize_project_descriptor(wt)
                    for wt in self.viztrails.list_viztrails()
            ],
            JSON_REFERENCES : [
                self_reference(self.urls.projects_url()),
                reference(REL_CREATE, self.urls.projects_url()),
                reference(REL_SERVICE, self.urls.service_url())
            ]
        }

    def serialize_project_descriptor(self, viztrail):
        """Create dictionary serialization for project fundamental project
        metadata.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle

        Returns
        -------
        dict
        """
        project_url = self.urls.project_url(viztrail.identifier)
        properties = viztrail.properties.get_properties()
        return {
            'id': viztrail.identifier,
            'environment': viztrail.env_id,
            'createdAt': viztrail.created_at.isoformat(),
            'lastModifiedAt': viztrail.last_modified_at.isoformat(),
            'properties': [
                {'key' : key, 'value' : properties[key]}
                    for key in properties
            ],
            JSON_REFERENCES : [
                self_reference(project_url),
                reference(REL_DELETE, project_url),
                reference(REL_SERVICE, self.urls.service_url()),
                reference(
                    REL_UPDATE,
                    self.urls.update_project_properties_url(viztrail.identifier)
                ),
                reference(
                    REL_BRANCHES,
                    self.urls.branches_url(viztrail.identifier)
                ),
                reference(
                    REL_MODULE_SPECS,
                    self.urls.project_module_specs_url(viztrail.identifier)
                )
            ]
        }

    def serialize_project_handle(self, viztrail):
        """Create dictionary serialization for project handle.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle

        Returns
        -------
        dict
        """
        # Get the fundamental project information (descriptor)
        obj = self.serialize_project_descriptor(viztrail)
        # Add workflow information
        obj['branches'] = [
            self.serialize_branch_descriptor(
                viztrail,
                viztrail.branches[b]
            ) for b in viztrail.branches
        ]
        return obj

    def update_project_properties(self, project_id, properties):
        """Update the set of user-defined properties for a project with given
        identifier.

        Returns None if no project with given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        properties : dict
            Dictionary representing property update statements

        Returns
        -------
        dict
            Serialization of the project handle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Update properties that are associated with the viztrail. Make sure
        # that a new project name, if given, is not the empty string.
        if 'name' in properties:
            project_name = properties['name']
            if not project_name is None:
                if project_name == '':
                    raise ValueError('not a valid project name')
        viztrail.properties.update_properties(properties)
        # Return serialization for project handle.
        return self.serialize_project_handle(viztrail)

    # --------------------------------------------------------------------------
    # Workflows
    # --------------------------------------------------------------------------
    def append_module(self, project_id, branch_id, workflow_version, module_spec, before_id=-1):
        """Insert module to existing workflow and execute the resulting
        workflow. If before_id is equal or greater than zero the module will be
        inserted at the specified position in the workflow otherwise it is
        appended at the end of the workflow.

        Raise a ValueError if the given command does not specify a valid
        workflow command.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_spec : vizier.workflow.module.ModuleSpecification
            Specification of the workflow module
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative

        Returns
        -------
        dict
            Serialization of the modified workflow handle.
        """
        # Evaluate command against the HEAD of the current work trail branch.
        # The result is None if the viztrail or branch is unknown
        viztrail = self.viztrails.append_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            command=module_spec,
            before_id=before_id
        )
        if viztrail is None:
            return None
        # Get modified workflow to return workflo handle
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.versions[-1]
        )
        return self.serialize_workflow_handle(viztrail, workflow)

    def create_branch(self, project_id, branch_id, workflow_version, module_id, properties):
        """Create a new workflow branch for a given project. The version and
        module identifier specify the parent of the new branch. The new branch
        will have it's properties set according to the given dictionary.

        Returns None if the specified project does not exist. Raises ValueError
        if the specified branch or module identifier do not exists.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id: int
            Module identifier in given workflow version
        properties: dict()
            Properties for new workflow branch

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Create new branch. The result is None if the project of branch do not
        # exit.
        branch = self.viztrails.create_branch(
            viztrail_id=project_id,
            source_branch=branch_id,
            workflow_version=workflow_version,
            module_id=module_id,
            properties=properties
        )
        if branch is None:
            return None
        return self.serialize_branch_descriptor(viztrail, branch)

    def delete_branch(self, project_id, branch_id):
        """Delete the branch with the given identifier from the given
        project.

        Returns True if the branch existed and False if the project or branch
        are unknown.

        Raises ValueError if an attempt is made to delete the default branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier

        Returns
        -------
        bool
        """
        # Delete viztrail branch. The result is None if either the viztrail or
        # the branch does not exist.
        viztrail = self.viztrails.delete_branch(
            viztrail_id=project_id,
            branch_id=branch_id
        )
        return not viztrail is None

    def delete_module(self, project_id, branch_id, workflow_version, module_id):
        """Delete a module in a project workflow branch and execute the
        resulting workflow.

        Returns the modified workflow descriptor on success. The result is None
        if no project, branch, or module with given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id : int
            Module identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Delete module from the viztrail branch. The result is False if the
        # viztrail branch or module is unknown
        success = self.viztrails.delete_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            module_id=module_id
        )
        if not success:
            return None
        # Return workflow handle on success
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.versions[-1]
        )
        return self.serialize_workflow_handle(viztrail, workflow)

    def get_branch(self, project_id, branch_id):
        """Retrieve a branch from a given project.

        Returns None if the project or the branch do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        dict
            Serialization of the project workflow
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Return None if branch does not exist
        if not branch_id in viztrail.branches:
            return None
        return self.serialize_branch_handle(
            viztrail,
            viztrail.branches[branch_id]
        )

    def get_workflow(self, project_id, branch_id, workflow_version=-1):
        """Retrieve a workflow from a given project.

        Returns None if no project, branch, or workflow with given identifiers
        exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int, optional
            Version number of the modified workflow

        Returns
        -------
        dict
            Serialization of the project workflow
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Retrieve workflow from repository. The result is None if the branch
        # does not exist.
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version
        )
        if workflow is None:
            return None
        return self.serialize_workflow_handle(viztrail, workflow)

    def list_branches(self, project_id):
        """Get a list of all branches for a given project. The result contains a
        list of branch descriptors. The result is None, if the specified project
        does not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        branches_url = self.urls.branches_url(project_id)
        return {
            'branches':  [
                self.serialize_branch_descriptor(
                    viztrail,
                    viztrail.branches[b]
                ) for b in viztrail.branches
            ],
            JSON_REFERENCES : [
                self_reference(branches_url),
                reference(REL_CREATE, branches_url),
                reference(REL_PROJECT, self.urls.project_url(project_id))
            ]
        }

    def replace_module(self, project_id, branch_id, workflow_version, module_id, module_spec):
        """Replace a module in a project workflow and execute the result.

        Raise a ValueError if the given command does not specify a valid
        workflow command.

        Returns None if no project with given identifier exists or if the
        specified module identifier is unknown.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        workflow_version: int
            Version number of the modified workflow
        module_id : int
            Module identifier
        module_spec : vizier.workflow.module.ModuleSpecification
            Specification of the workflow module

        Returns
        -------
        dict
            Serialization of the modified workflow handle.
        """
        # Evaluate command against the HEAD of the current work trail branch.
        # The result is None if the viztrail or branch is unknown
        viztrail = self.viztrails.replace_workflow_module(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=workflow_version,
            module_id=module_id,
            command=module_spec
        )
        if viztrail is None:
            return None
        # Get modified workflow to return workflow handle
        branch = viztrail.branches[branch_id]
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=branch.versions[-1]
        )
        return self.serialize_workflow_handle(viztrail, workflow)

    def serialize_branch_descriptor(self, viztrail, branch):
        """Get dictionary representaion for a branch descriptor.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle
        branch : vizier.workflow.base.ViztrailBranch
            Workflow handle

        Returns
        -------
        dict
        """
        self_ref = self.urls.branch_url(
            viztrail.identifier,
            branch.identifier
        )
        head_ref = self.urls.branch_head_url(
            viztrail.identifier,
            branch.identifier
        )
        properties = branch.properties.get_properties()
        return {
            'id' : branch.identifier,
            'properties' : [
                {'key' : key, 'value' : properties[key]}
                    for key in properties
            ],
            JSON_REFERENCES : [
                self_reference(self_ref),
                reference(REL_DELETE, self_ref),
                reference(REL_HEAD, head_ref),
                reference(
                    REL_PROJECT,
                    self.urls.project_url(viztrail.identifier)
                ),
                reference(
                    REL_UPDATE,
                    self.urls.branch_update_url(
                        viztrail.identifier,
                        branch.identifier
                    )
                )
            ]
        }

    def serialize_branch_handle(self, viztrail, branch):
        """Get dictionary representaion for a branch handle.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle
        branch : vizier.workflow.base.ViztrailBranch
            Workflow handle

        Returns
        -------
        dict
        """
        obj = self.serialize_branch_descriptor(viztrail, branch)
        obj['project'] = self.serialize_project_descriptor(viztrail)
        obj['workflows'] = [
            self.serialize_workflow_descriptor(viztrail, branch, version)
                for version in branch.versions
        ]
        return obj

    def serialize_dataset_instance(self, dataset_id, dataset_name):
        """Create dictionary serialization for dataset instance.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier
        dataset_name : string
            Name used to identify dataset

        Returns
        -------
        dict
        """
        return {
            'name' : dataset_name,
            JSON_REFERENCES : [
                self_reference(self.urls.dataset_url(dataset_id)),
                reference(
                    REL_ANNOTATED,
                    self.urls.dataset_with_annotations_url(dataset_id)
                ),
                reference(
                    REL_DOWNLOAD,
                    self.urls.dataset_download_url(dataset_id)
                )
            ]
        }

    def serialize_module_handle(self, viztrail, branch, version, module):
        """Get dictionary representaion for a workflow module handle.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle
        branch : vizier.workflow.base.ViztrailBranch
            Workflow handle
        module : vizier.workflow.module.ModuleHandle
            Handle for workflow module

        Returns
        -------
        dict
        """
        module_url = self.urls.workflow_module_url(
            viztrail.identifier,
            branch.identifier,
            version,
            module.identifier
        )
        args = module.command.arguments
        return {
            'id' : module.identifier,
            'command': {
                'type': module.command.module_type,
                'id': module.command.command_identifier,
                'arguments': [{'name': key, 'value': args[key]} for key in args]
            },
            'stdout': module.stdout,
            'stderr': module.stderr,
            'datasets': [
                self.serialize_dataset_instance(module.datasets[d], d)
                    for d in sorted(module.datasets.keys())
            ],
            JSON_REFERENCES: [
                reference(REL_DELETE, module_url),
                reference(REL_INSERT, module_url),
                reference(REL_REPLACE, module_url)
            ]
        }

    def serialize_workflow_descriptor(self, viztrail, branch, version):
        """Get dictionary representaion for a workflow descriptor.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle
        branch : vizier.workflow.base.ViztrailBranch
            Workflow handle

        Returns
        -------
        dict
        """
        # The workflow version may be negative for the HEAD of an empty master
        # branch (i.e., a newly created viztrail). In this cas we use a
        # different Url
        if version < 0:
            self_ref = self.urls.branch_head_url(
                viztrail.identifier,
                branch.identifier
            )
            append_url = self.urls.branch_head_append_url(
                viztrail.identifier,
                branch.identifier
            )
        else:
            self_ref = self.urls.workflow_url(
                viztrail.identifier,
                branch.identifier,
                version
            )
            append_url = self.urls.workflow_append_url(
                viztrail.identifier,
                branch.identifier,
                version
            )

        return {
            'branch' : branch.identifier,
            'version' : version,
            JSON_REFERENCES : [
                self_reference(self_ref),
                reference(
                    REL_BRANCH,
                    self.urls.branch_url(viztrail.identifier, branch.identifier)
                ),
                reference(
                    REL_BRANCHES,
                    self.urls.branches_url(viztrail.identifier)
                ),
                reference(REL_APPEND, append_url)
            ]
        }

    def serialize_workflow_handle(self, viztrail, workflow):
        """Get dictionary representaion for a workflow handle.

        Parameters
        ----------
        viztrail : vizier.workflow.base.ViztrailHandle
            Viztrail handle
        workflow : vizier.workflow.base.WorkflowHandle
            Workflow handle

        Returns
        -------
        dict
        """
        branch = viztrail.branches[workflow.branch_id]
        version = workflow.version
        obj = self.serialize_workflow_descriptor(viztrail, branch, version)
        obj['createdAt'] = workflow.created_at.isoformat()
        obj['project'] = self.serialize_project_descriptor(viztrail)
        # Add module listing
        obj['modules'] = [
            self.serialize_module_handle(viztrail, branch, version, m)
                for m in workflow.modules
        ]
        return obj

    def update_branch(self, project_id, branch_id, properties):
        """Update properties for a given project workflow branch. Returns the
        handle for the modified workflow or None if the project or branch do not
        exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        properties: dict()
            Properties that are being updated. A None value for a property
            indicates that the property is to be deleted.

        Returns
        -------
        dict
        """
        # Get the viztrail to ensure that it exists
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Get the specified branch
        if not branch_id in viztrail.branches:
            return None
        # Update properties that are associated with the workflow
        viztrail.branches[branch_id].properties.update_properties(properties)
        return self.serialize_workflow_handle(
            viztrail,
            viztrail.get_workflow(branch_id)
        )

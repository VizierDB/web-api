"""Collection of functions that serialize Vizier data objects."""

from vizier.core.properties import ObjectProperty
from vizier.hateoas import reference, self_reference
from vizier.plot.view import ChartViewHandle

import vizier.hateoas as hateoas
import vizier.workflow.command as cmd


"""Frequently used serialization element labels."""
JSON_REFERENCES = 'links'


"""Service properties"""
# Fileserver - Max. upload file size
PROP_FS_MAXFILESIZE = 'fileserver:maxFileSize'


"""Module output content types."""
O_CHARTVIEW = 'chart/view'
O_PLAINTEXT = 'text/plain'


def BRANCH_DESCRIPTOR(viztrail, branch, urls):
    """Dictionary representaion for a branch descriptor.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    branch : vizier.workflow.base.ViztrailBranch
        Workflow handle
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    vt_id = viztrail.identifier
    branch_id = branch.identifier
    properties = branch.properties.get_properties()
    self_ref = urls.branch_url(vt_id, branch_id)
    head_ref = urls.branch_head_url(vt_id, branch_id)
    project_ref = urls.project_url(vt_id)
    update_ref =  urls.branch_update_url(vt_id, branch_id)
    return {
        'id' : branch_id,
        'properties' : [
            {'key' : key, 'value' : properties[key]}
                for key in properties
        ],
        JSON_REFERENCES : [
            self_reference(self_ref),
            reference(hateoas.REL_DELETE, self_ref),
            reference(hateoas.REL_HEAD, head_ref),
            reference(hateoas.REL_PROJECT, project_ref),
            reference(hateoas.REL_UPDATE, update_ref)
        ]
    }


def BRANCH_HANDLE(viztrail, branch, urls):
    """Dictionary representaion for a branch handle.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    branch : vizier.workflow.base.ViztrailBranch
        Workflow handle
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    obj = BRANCH_DESCRIPTOR(viztrail, branch, urls)
    obj['project'] = PROJECT_DESCRIPTOR(viztrail, urls)
    obj['workflows'] = []
    for wf in branch.workflows:
        descriptor = WORKFLOW_DESCRIPTOR(
            viztrail,
            branch,
            wf.version,
            wf.created_at,
            urls
        )
        if not wf.package_id is None:
            descriptor['packageId'] = wf.package_id
        if not wf.command_id is None:
            descriptor['commandId'] = wf.command_id
        obj['workflows'].append(descriptor)
    return obj


def BRANCH_LISTING(viztrail, urls):
    """Dictionary serialization for listing of viztrail branches.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    vt_id = viztrail.identifier
    branches_url = urls.branches_url(vt_id)
    return {
        'branches':  [
            BRANCH_DESCRIPTOR(
                viztrail,
                viztrail.branches[b],
                urls
            ) for b in viztrail.branches
        ],
        JSON_REFERENCES : [
            self_reference(branches_url),
            reference(hateoas.REL_CREATE, branches_url),
            reference(hateoas.REL_PROJECT, urls.project_url(vt_id))
        ]
    }


def CHART_VIEW(view, rows):
    """Create chart view output from a given handle.

    Parameters
    ----------
    view: vizier.plot.view.ChartViewHandle
        Handle defining the dataset chart view
    rows: list()
        List of rows in the query result

    Returns
    -------
    dict
    """
    return {
        'type': O_CHARTVIEW,
        'data': view.to_dict(),
        'result': CHART_VIEW_DATA(view=view, rows=rows)
    }


def CHART_VIEW_DATA(view, rows):
    """Create a dictionary serialization of daraset chart view results. The
    output is a dictionary with the following format (the xAxis element is
    optional):

    {
        "series": [{
            "label": "string",
            "data": [0]
        }],
        "xAxis": {
            "data": [0]
        }
    }

    Parameters
    ----------
    view: vizier.plot.view.ChartViewHandle
        Handle defining the dataset chart view
    rows: list()
        List of rows in the query result

    Returns
    -------
    dict
    """
    obj = dict()
    # Add chart type information
    obj['chart'] = {
        'type': view.chart_type,
        'grouped': view.grouped_chart
    }
    # Create a list of series indexes. Then remove the series that contains the
    # x-axis labels (if given). Keep x-axis data in a separate list
    series = range(len(view.data))
    if not view.x_axis is None:
        obj['xAxis'] = {'data': [row[view.x_axis] for row in rows]}
        del series[view.x_axis]
    obj['series'] = list()
    for s_idx in series:
        obj['series'].append({
            'label': view.data[s_idx].label,
            'data': [row[s_idx] for row in rows]
        })
    return obj


def COMMAND_REPOSITORY(command_repository):
    """List of dictionary serializations for all module specifications in the
    given repository.

    Parameters:
    -----------
    command_repository: dict
        Dictionary of module packages

    Returns
    -------
    dict
    """
    modules = list()
    for module_type in command_repository:
        type_commands = command_repository[module_type]
        for command_id in type_commands:
            modules.append(
                MODULE_SPECIFICATION(
                    module_type,
                    command_id,
                    type_commands[command_id]
                )
            )
    return modules


def DATASET(dataset, rows, config, urls, offset=0, limit=-1, include_annotations=False):
    """Dictionary serialization for (part of the ) dataset state.

    Parameters
    ----------
    dataset : vizier.datastore.base.DatasetHandle
        Handle for dataset
    rows: list()
        List of rows from the dataset
    config : vizier.config.AppConfig
        Application configuration parameters
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls
    offset: int, optional
        Number of rows at the beginning of the list that are skipped.
    limit: int, optional
        Limits the number of rows that are returned.
    include_annotations: bool
        Flag indicating whether dataset annotations should be included in
        the result

    Returns
    -------
    dict
    """
    dataset_id = dataset.identifier
    # Serialize the dataset schema and cells
    obj = {
        'id' : dataset_id,
        'columns' : [col.to_dict() for col in dataset.columns],
        'rows': rows,
        'offset': offset,
        'rowcount': dataset.row_count
    }
    if include_annotations:
        obj['annotations'] = DATASET_ANNOTATIONS(
            dataset_id,
            dataset.annotations,
            urls
        )
    # Add references if dataset exists
    obj[JSON_REFERENCES] = [
        self_reference(urls.dataset_url(dataset_id)),
        reference(
            hateoas.REL_DOWNLOAD,
            urls.dataset_download_url(dataset_id)
        ),
        reference(
            hateoas.REL_ANNOTATIONS,
            urls.dataset_annotations_url(dataset_id)
        )
    ] + DATASET_PAGINATION_URLS(dataset, config, urls, offset=offset, limit=limit)
    return obj


def DATASET_ANNOTATIONS(dataset_id, annotations, urls):
    """Get dictionary serialization for dataset annotations.

    Parameters
    ----------
    dataset_id : string
        Unique dataset identifier
    annotations: vizier.datastore.metadata.DatasetMetadata
        Set of annotations for dataset components
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    obj = annotations.to_dict()
    # Add references if dataset exists
    obj[JSON_REFERENCES] = [
        self_reference(urls.dataset_annotations_url(dataset_id)),
        reference(hateoas.REL_DATASET, urls.dataset_url(dataset_id))
    ]
    return obj


def DATASET_CHART_VIEW(view, rows, self_ref):
    """Dictionary serialization for chart view data results.

    Parameters
    ----------
    view: vizier.plot.view.ChartViewHandle
        Handle defining the dataset chart view
    rows: list()
        List of rows in the query result
    self_ref: string
        Self-reference url for chart view data.

    Returns
    -------
    dict
    """
    obj = CHART_VIEW_DATA(view=view, rows=rows)
    obj['name'] = view.chart_name
    obj[JSON_REFERENCES] = [self_reference(self_ref)]
    return obj


def DATASET_DESCRIPTOR(dataset, config, urls):
    """Create dictionary serialization for dataset descriptor.

    Parameters
    ----------
    dataset : vizier.datastore.base.DatasetHandle
        Handle for dataset
    config : vizier.config.AppConfig
        Application configuration parameters
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    dataset_id = dataset.identifier
    return {
        'id': dataset_id,
        'columns' : [
            {'id': col.identifier, 'name': col.name}
                for col in dataset.columns
        ],
        'rows': dataset.row_count,
        JSON_REFERENCES : [
            self_reference(urls.dataset_url(dataset_id)),
            reference(
                hateoas.REL_ANNOTATED,
                urls.dataset_with_annotations_url(dataset_id)
            ),
            reference(
                hateoas.REL_DOWNLOAD,
                urls.dataset_download_url(dataset_id)
            )
        ] + DATASET_PAGINATION_URLS(dataset, config, urls)
    }


def DATASET_PAGE_URLS(dataset, rel, offset, limit, urls):
    """Get a pair of Urls to access a specific page of a dataset. the result
    contains one Url to access the data with annotations and one Url to
    access the data without annotations.

    Parameters
    ----------
    dataset: vizier.datastore.base.DatasetHandle
        Handle for the dataset
    rel: string
        HATEOS reference relationship
    offset: int
        Current pagination offset
    limit: int
        Current paginatio limit
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    list
    """
    # Shortcuts
    d_id = dataset.identifier
    url = urls.dataset_pagination_url
    # Return list with two references
    return [
        reference(rel, url(d_id, offset=offset, limit=limit)),
        reference(
            rel + 'anno',
            url(d_id, offset=offset, limit=limit, include_annotations=True)
        )
    ]


def DATASET_PAGINATION_URLS(dataset, config, urls, offset=0, limit=None):
    """Get a list of dataset references to allow browsing the dataset rows.

    Parameters
    ----------
    dataset: vizier.datastore.base.DatasetHandle
        Handle for the dataset
    config : vizier.config.AppConfig
        Application configuration parameters
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls
    offset: int, optional
        Current pagination offset
    limit: int, optional
        Current paginatio limit

    Returns
    -------
    list()
    """
    # Max. number of records shown
    if not limit is None and limit >= 0:
        max_rows_per_request = int(limit)
    elif config.defaults.row_limit >= 0:
        max_rows_per_request = config.defaults.row_limit
    elif config.defaults.max_row_limit >= 0:
        max_rows_per_request = config.defaults.max_row_limit
    else:
        max_rows_per_request = -1
    # List of pagination Urls
    page_urls = list()
    # FIRST: Always include Url's to access the first page
    page_urls.extend(
        DATASET_PAGE_URLS(
            dataset,
            rel=hateoas.REL_PAGE_FIRST,
            offset=0,
            limit=limit,
            urls=urls
        )
    )
    # PREV: If offset is greater than zero allow to fetch previous page
    if not offset is None and offset > 0:
        if max_rows_per_request >= 0:
            prev_offset = offset - max_rows_per_request
            if prev_offset >= 0:
                page_urls.extend(
                    DATASET_PAGE_URLS(
                        dataset,
                        rel=hateoas.REL_PAGE_PREV,
                        offset=prev_offset,
                        limit=limit,
                        urls=urls
                    )
                )
    # NEXT & LAST: If there are rows beyond the current offset+limit include
    # Url's to fetch next page and last page.
    if offset < dataset.row_count and max_rows_per_request >= 0:
        next_offset = offset + max_rows_per_request
        if next_offset < dataset.row_count:
            page_urls.extend(
                DATASET_PAGE_URLS(
                    dataset,
                    rel=hateoas.REL_PAGE_NEXT,
                    offset=next_offset,
                    limit=limit,
                    urls=urls
                )
            )
        last_offset = (dataset.row_count - max_rows_per_request)
        if last_offset > offset:
            page_urls.extend(
                DATASET_PAGE_URLS(
                    dataset,
                    rel=hateoas.REL_PAGE_LAST,
                    offset=last_offset,
                    limit=limit,
                    urls=urls
                )
            )
    # Return pagination Url list
    return page_urls


def FILE_HANDLE(f_handle, urls):
    """Dictionary serialization for dataset instance.

    Parameters
    ----------
    f_handle : database.fileserver.FileHandle
        Handle for file server resource
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    self_ref = urls.file_url(f_handle.identifier)
    return {
        'id': f_handle.identifier,
        'name' : f_handle.name,
        'columns' : f_handle.columns,
        'rows': f_handle.rows,
        'filesize': f_handle.filesize,
        'createdAt': f_handle.created_at.isoformat(),
        'lastModifiedAt': f_handle.last_modified_at.isoformat(),
        JSON_REFERENCES : [
            self_reference(self_ref),
            reference(hateoas.REL_DELETE, self_ref),
            reference(hateoas.REL_RENAME, self_ref),
            reference(
                hateoas.REL_DOWNLOAD,
                urls.file_download_url(f_handle.identifier)
            )
        ]
    }


def FILE_LISTING(files, urls):
    """Dictionary serialization for file listing.

    Parameters
    ----------
    files: list(vizier.filestore.base.FileHandle)
        List of file handles
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    return {
        'files': [FILE_HANDLE(f, urls) for f in files],
        JSON_REFERENCES : [
            self_reference(urls.files_url()),
            reference(hateoas.REL_UPLOAD, urls.files_upload_url())
        ]
    }


def PLAIN_TEXT(text):
    """Create a plain text output object.

    Parameters
    ----------
    text: string
        Plain output text

    Returns
    -------
    dict
    """
    return {'type': O_PLAINTEXT, 'data': text}


def PROJECT_DESCRIPTOR(viztrail, urls):
    """Dictionary serialization for project fundamental project metadata.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_url = urls.project_url(viztrail.identifier)
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
            reference(hateoas.REL_DELETE, project_url),
            reference(hateoas.REL_SERVICE, urls.service_url()),
            reference(
                hateoas.REL_UPDATE,
                urls.update_project_properties_url(viztrail.identifier)
            ),
            reference(
                hateoas.REL_BRANCHES,
                urls.branches_url(viztrail.identifier)
            ),
            reference(
                hateoas.REL_MODULE_SPECS,
                urls.project_module_specs_url(viztrail.identifier)
            )
        ]
    }


def PROJECT_HANDLE(viztrail, files, urls, branch_id=None, version=None):
    """Dictionary serialization for project handle.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    files: list(vizier.filestore.base.FileHandle)
        List of file handles
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls
    branch_id: string
        Unique branch identifier
    version: int, optional
        Workflow version identifier

    Returns
    -------
    dict
    """
    # Get the fundamental project information (descriptor)
    obj = PROJECT_DESCRIPTOR(viztrail, urls)
    # Add listing of module specifications and available files
    obj['environment'] = {
        'id': viztrail.env_id,
        'modules': COMMAND_REPOSITORY(viztrail.command_repository),
        'files': [{'id': f.identifier, 'name' : f.name} for f in files]
    }
    # Add branch information and workflow links
    obj['branches'] = []
    for b in viztrail.branches:
        branch = viztrail.branches[b]
        obj['branches'].append(BRANCH_DESCRIPTOR(viztrail, branch, urls))
    # Include reference to requested workflow if branch_id and version are
    # given. Does not check if the resource exists
    if not branch_id is None and not version is None:
        is_head = False
        try:
            is_head = (int(version) < 0)
        except ValueError:
            pass
        if is_head:
            url = urls.branch_head_url(viztrail.identifier, str(branch_id))
        else:
            url = urls.workflow_url(viztrail.identifier, str(branch_id), version)
        obj[JSON_REFERENCES].append(reference(hateoas.REL_WORKFLOW, url))
    return obj


def PROJECT_LISTING(projects, urls):
    """Dictionary serialization of a project listing.

    Parameters
    ----------
    projects: list(vizier.workflow.base.ViztrailHandle)
        List of viztrail descriptors
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    return {
        'projects' : [PROJECT_DESCRIPTOR(wt, urls) for wt in projects],
        JSON_REFERENCES : [
            self_reference(urls.projects_url()),
            reference(hateoas.REL_CREATE, urls.projects_url()),
            reference(hateoas.REL_SERVICE, urls.service_url())
        ]
    }


def PROJECT_MODULE_SPECIFICATIONS(viztrail, urls):
    """Dictionary serialization for project module specifications.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    return {
        'project': PROJECT_DESCRIPTOR(viztrail, urls),
        'modules': COMMAND_REPOSITORY(viztrail.command_repository),
        JSON_REFERENCES : [
            self_reference(urls.project_module_specs_url(viztrail.identifier))
        ]
    }


def MODULE_HANDLE(viztrail, branch, version, module, views, urls):
    """Get dictionary representaion for a workflow module handle.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    branch : vizier.workflow.base.ViztrailBranch
        Workflow handle
    module : vizier.workflow.module.ModuleHandle
        Handle for workflow module
    views: dict(vizier.plot.view.ChartViewHandle)
        Dictionary of available views indexed by their name.

    Returns
    -------
    dict
    """
    module_url = urls.workflow_module_url(
        viztrail.identifier,
        branch.identifier,
        version,
        module.identifier
    )

    # Convert chart views in the module output to dictionaries that contain
    # a self reference for data access. In the first step we replace the
    # data value with the view name
    stdout = list()
    view_outputs = list()
    for obj in module.stdout:
        if obj['type'] == O_CHARTVIEW:
            view = ChartViewHandle.from_dict(obj['data'])
            if view.dataset_name in module.datasets:
                views[view.chart_name] = view
                # This is a bit tricky. Create a placeholder object and then
                # replace the data value with the serialized version of
                # the chart handle later on. Make sure to keep track of any
                # results that may be associated with the output
                placeholder = {'type': O_CHARTVIEW, 'data': view.chart_name}
                if 'result' in obj:
                    placeholder['result'] = obj['result']
                obj = placeholder
                view_outputs.append(obj)
            else:
                # Remove outputs that reference views accessing non-existent
                # datasets
                obj = None
        if not obj is None:
            stdout.append(obj)
    # Create a list of serialized view handles
    view_handles = dict()
    for view in views.values():
        if view.dataset_name in module.datasets:
            view_url = urls.workflow_module_view_url(
                viztrail.identifier,
                branch.identifier,
                version,
                module.identifier,
                view.identifier
                )
            v_serial = {
                'name': view.chart_name,
                JSON_REFERENCES: [
                    self_reference(view_url)
                ]
            }
            view_handles[view.chart_name] = v_serial
    # Replace data in view outputs
    for obj in view_outputs:
        obj['data'] = view_handles[obj['data']]
    args = module.command.arguments
    return {
        'id' : module.identifier,
        'command': {
            'type': module.command.module_type,
            'id': module.command.command_identifier,
            'arguments': [{'name': key, 'value': args[key]} for key in args]
        },
        'stdout': stdout,
        'stderr': module.stderr,
        'datasets': [{
                'id': module.datasets[d],
                'name' : d
            } for d in sorted(module.datasets.keys())
        ],
        'views': view_handles.values(),
        JSON_REFERENCES: [
            reference(hateoas.REL_DELETE, module_url),
            reference(hateoas.REL_INSERT, module_url),
            reference(hateoas.REL_REPLACE, module_url)
        ]
    }



def MODULE_SPECIFICATION(module_type, module_id, module_spec):
    """Dictionary serialization for a workflow module specification.

    Parameters
    ----------
    module_type: string
        Identifier of the package the module belongs to
    module_id: string
        Package-specific unique identifier of the module
    module_spec: dict
        Module specification. Is expected toe contain 'name' and 'arguments'

    Returns
    -------
    dict
    """
    arguments =  module_spec[cmd.MODULE_ARGUMENTS]
    return {
        'type': module_type,
        'id': module_id,
        'name': module_spec[cmd.MODULE_NAME],
        'arguments': [arguments[arg] for arg in arguments]
    }


def SERVICE_BUILD(components, urls):
    """Dictionary serialization for service build information.

    Parameters
    ----------
    components: dict
        Dictionary serialization of individual components
    url

    Returns
    -------
    dict
    """
    return {
        'components' : components,
        JSON_REFERENCES : [
            self_reference(urls.system_build_url()),
            reference(hateoas.REL_SERVICE, urls.service_url())
        ]
    }


def SERVICE_DESCRIPTOR(config, urls):
    """Dictionary serialization for service configuration and references.

    Parameters
    ----------
    config : vizier.config.AppConfig
        Application configuration parameters
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    return {
        'name' : config.name,
        'properties': [
            ObjectProperty(
                PROP_FS_MAXFILESIZE,
                config.fileserver.max_file_size
            ).to_dict()
            ],
        'envs': [{
                'id': config.envs[key].identifier,
                'name': config.envs[key].name,
                'description': config.envs[key].description,
                'default': config.envs[key].default,
                'packages': config.envs[key].packages
            } for key in config.envs
        ],
        JSON_REFERENCES : [
            self_reference(urls.service_url()),
            reference(hateoas.REL_SYSTEM_BUILD, urls.system_build_url()),
            reference(hateoas.REL_PROJECTS, urls.projects_url()),
            reference(hateoas.REL_FILES, urls.files_url()),
            reference(hateoas.REL_UPLOAD, urls.files_upload_url()),
            reference(hateoas.REL_APIDOC, config.api.doc_url)
        ]
    }


def WORKFLOW_DESCRIPTOR(viztrail, branch, version, created_at, urls):
    """Get dictionary representaion for a workflow descriptor.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    branch : vizier.workflow.base.ViztrailBranch
        Workflow handle
    version: int
        Workflow version identifier
    created_at: datetime.datetime
        Timestamp for workflow creation
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    # The workflow version may be negative for the HEAD of an empty master
    # branch (i.e., a newly created viztrail). In this cas we use a
    # different Url
    vt_id = viztrail.identifier
    branch_id = branch.identifier
    if version < 0:
        self_ref = urls.branch_head_url(vt_id, branch_id)
        append_url = urls.branch_head_append_url(vt_id, branch_id)
    else:
        self_ref = urls.workflow_url(vt_id, branch_id, version)
        append_url = urls.workflow_append_url(vt_id, branch_id, version)
    # Return  serialization
    return {
        'version': version,
        'createdAt': created_at.isoformat(),
        JSON_REFERENCES: [
            self_reference(self_ref),
            reference(hateoas.REL_BRANCH, urls.branch_url(vt_id, branch_id)),
            reference(hateoas.REL_BRANCHES, urls.branches_url(vt_id)),
            reference(hateoas.REL_APPEND, append_url)
        ]
    }


def WORKFLOW_HANDLE(viztrail, workflow, config, urls, dataset_cache, read_only=False):
    """Dictionary representaion for a workflow handle.

    Parameters
    ----------
    viztrail : vizier.workflow.base.ViztrailHandle
        Viztrail handle
    workflow : vizier.workflow.base.WorkflowHandle
        Workflow handle
    config : vizier.config.AppConfig
        Application configuration parameters
    urls: vizier.hateoas.UrlFactory
        Factory for resource urls
    dataset_cache: func
        Function to get dataset handle for given identifier
    read_only: bool, oprional
        Value for the read only flag in the workflow serialization
    Returns
    -------
    dict
    """
    branch = viztrail.branches[workflow.branch_id]
    version = workflow.version
    created_at = workflow.created_at
    obj = WORKFLOW_DESCRIPTOR(viztrail, branch, version, created_at, urls)
    obj['project'] = PROJECT_DESCRIPTOR(viztrail, urls)
    obj['branch'] = BRANCH_DESCRIPTOR(viztrail, branch, urls)
    # Create listing of workflow modules. This will transform chart view
    # outputs into web resources and keep track of views that are available
    # to each module.
    views = dict()
    obj['modules'] = [
        MODULE_HANDLE(viztrail, branch, version, module, views, urls)
            for module in workflow.modules
    ]
    # Create list of all datasets in the workflow.
    datasets = dict()
    for module in workflow.modules:
        for dataset_id in module.datasets.values():
            if not dataset_id in datasets:
                dataset = dataset_cache(dataset_id)
                datasets[dataset_id] = DATASET_DESCRIPTOR(dataset, config, urls)
    obj['datasets'] = datasets.values()
    obj['readOnly'] = read_only
    return obj

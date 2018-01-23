"""Hypermedia As The Engine of Application State - Factory for Web API resource
Urls.

The Web API attempts to follow the Hypermedia As The Engine of Application State
(HATEOAS) constraint. Thus, every serialized resource contains a list of
references for clients to interact with the API.

The URLFactory class in this s contains all methods to generate HATEOAS
references for resources that are accessible via the Vizier Web API.
"""


class UrlFactory:
    """Factory for API resource Urls. Contains the definitions of Url's for any
    resource that is accessible through the Web API in a single class.

    Attributes
    ----------
    base_url: string
        Prefix for all resource Url's
    """
    def __init__(self, config):
        """Intialize the common Url prefix for all API resources.

        Parameters
        ----------
        config: vizier.config.AppConfig
            Application configuration parameters
        """
        # Construct base Url from server url, port, and application path.
        self.base_url = config.api.app_base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    def branches_url(self, project_id):
        """Url to retrieve (GET) the list of branches for a project with the
        given identifier.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/branches'

    def branch_url(self, project_id, branch_id):
        """Url to retrieve (GET) the branch with given identifier for a given
        project.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branches_url(project_id) + '/' + branch_id

    def branch_head_url(self, project_id, branch_id):
        """Url to access the workflow at the branch HEAD.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_url(project_id, branch_id) + '/head'

    def branch_head_append_url(self, project_id, branch_id):
        """Url to append a module to the workflow at the branch HEAD.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_head_url(project_id, branch_id) + '/modules'

    def branch_update_url(self, project_id, branch_id):
        """Url to update (POST) the properties of a workflow branch.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier

        Returns
        -------
        string
        """
        return self.branch_url(project_id, branch_id) + '/properties'

    def datasets_url(self):
        """Base Url for dataset resources.

        Returns
        -------
        string
        """
        return self.base_url + '/datasets'

    def datasets_upload_url(self):
        """Url to create dataset from CSV or TSV file.

        Returns
        -------
        string
        """
        return self.datasets_url()

    def dataset_url(self, dataset_id):
        """Url to retrieve dataset state in Json format.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.datasets_url() + '/' + dataset_id

    def dataset_annotations_url(self, dataset_id):
        """Url to retrieve dataset annotations.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '/annotations'

    def dataset_download_url(self, dataset_id):
        """Url to retrieve a dataset in CSV format.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '/csv'

    def dataset_with_annotations_url(self, dataset_id):
        """Url to retrieve a dataset together with all of its annotations.

        Parameters
        ----------
        dataset_id : string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.dataset_url(dataset_id) + '?includeAnnotations=true'

    def files_url(self):
        """Base Url for file server resources.

        Returns
        -------
        string
        """
        return self.base_url + '/files'

    def files_upload_url(self):
        """Url to upload CSV or TSV file.

        Returns
        -------
        string
        """
        return self.files_url() + '/upload'

    def file_url(self, name):
        """Url to retrieve uploaded file handle.

        Parameters
        ----------
        name : string
            File name

        Returns
        -------
        string
        """
        return self.files_url() + '/' + name

    def file_download_url(self, name):
        """Url to retrieve a file server resource in CSV format.

        Parameters
        ----------
        name : string
            File name

        Returns
        -------
        string
        """
        return self.file_url(name) + '/download'

    def projects_url(self):
        """Url to retrieve project listing (GET) and to create new project
        (POST).

        Returns
        -------
        string
        """
        return self.service_url() + '/projects'

    def project_url(self, project_id):
        """Url to retrieve (GET) or delete (DELETE) project with given
        identifier.

        Returns
        -------
        string
        """
        return self.projects_url() + '/' + project_id

    def project_module_specs_url(self, project_id):
        """Url to retrieve the list of available module specification for a
        given project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/modulespecs'

    def service_url(self):
        """Base Url for the Web API server.

        Returns
        -------
        string
        """
        return self.base_url

    def system_build_url(self):
        """Url to retrieve system configuration information.

        Returns
        -------
        string
        """
        return self.service_url() + '/build'

    def update_project_properties_url(self, project_id):
        """Url to update (POST) the set of user-defined properties for the
        project with given identifier.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.project_url(project_id) + '/properties'

    def workflow_url(self, project_id, branch_id, version):
        """Url to retrieve (GET) a project workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int
            Workflow version identifier

        Returns
        -------
        string
        """
        branch_url = self.branch_url(project_id, branch_id)
        return branch_url + '/workflows/' + str(version)

    def workflow_append_url(self, project_id, branch_id, version):
        """Url to retrieve (POST) to append a module at the end of a given
        workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        version: int
            Workflow version identifier

        Returns
        -------
        string
        """
        return self.workflow_url(project_id, branch_id, version) + '/modules'

    def workflow_module_url(self, project_id, branch_id, version, module_id):
        """Url for workflow module. Used to delete, insert, and replace a
        module in a workflow.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Workflow branch identifier
        version: int
            Workflow version identifier
        module_id: int
            Unique module identifier

        Returns
        -------
        string
        """
        workflow_url = self.workflow_url(project_id, branch_id, version)
        return workflow_url + '/modules/' + str(module_id)


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def reference(rel, href):
    """Get HATEOAS reference object containing the Url 'href' and the link
    relation 'rel' that defines the type of the link.

    Parameters
    ----------
    rel : string
        Descriptive attribute defining the link relation
    href : string
        Http Url

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return {'rel' : rel, 'href' : href}


def self_reference(url):
    """Get HATEOAS self reference for a API resources.

    Parameters
    ----------
    url : string
        Url to resource

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return reference('self', url)

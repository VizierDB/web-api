"""Definition of basic system components."""

from abc import abstractmethod

from vizier.core import VERSION_INFO


class VizierSystemComponent(object):
    """Components in the Vizier-DB Data Cutation System maintain build
    information. The complete build information for all components in a running
    system is access via the Web API.
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
        for key in ['name', 'version']:
            if not key in build:
                raise ValueError('invalid build information: missing key \'' + key + '\'')
        self.build = build

    @abstractmethod
    def components(self):
        """List of component descriptor and sub-component descriptors (if any).

        Returns
        -------
        list
        """
        raise NotImplementedError

    def system_build(self):
        """Returns a dictionary containing the implementation-specific component
        name and version information.

        Returns
        -------
        dict
        """
        return {'name' : self.build['name'], 'version' : self.build['version']}



# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def build_info(name, version_info=None):
    """Return build information dictionary for Vizier system component.

    Parameters
    ----------
    name: string
        Component name
    version_info: strin, optional
        Version information. Defaults to 0.1.0

    Returns
    -------
    dict
    """
    v_info = version_info if not version_info is None else VERSION_INFO
    return {'name': name, 'version': v_info}


def component_descriptor(name, build):
    """Return component information dictionary containing component name and
    build information.

    Parameters
    ----------
    name: string
        Component name
    build: dict
        Component build information

    Returns
    -------
    dict
    """
    return {'name' : name, 'build' : build}

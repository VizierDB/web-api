"""Vizier DB Workflow API - Specification of workflow modules.
"""

class ModuleHandle(object):
    """Handle for a module in a curation workflow. Each module has a unique
    identifier, a specification of the executed command, list of generated
    outputs to STDOUT and STDERR, and a dictionary of resulting datasets.

    Attributes
    ----------
    identifier : int
        Unique module number (within a worktrail)
    command : vizier.workflow.module.ModuleSpecification
        Specification of the module (i.e., package, name, and arguments)
    datasets : dict(string)
        Dictionary of resulting datasets. the user-specified name is the key
        and the unique dataset identifier the value.
    stdout : list(string), optional
        Module output that was written to STDOUT
    stderr: list(string), optional
        Module output that was written to STDERR
    """
    def __init__(self, identifier, command, datasets=None, stdout=None, stderr=None):
        """initialize module properties. For new modules, datasets and outputs
        are initially empty

        Parameters
        ----------
        identifier : int
            Unique module number (within a worktrail)
        command : ModuleSpecification
            Specification of the module (i.e., package, name, and arguments)
        datasets : dict(string), optional
            Dictionary of resulting datasets. the user-specified name is the key
            and the unique dataset identifier the value.
        stdout : list(string), optional
            Module output that was written to STDOUT
        stderr: list(str), optional
            Module output that was written to STDERR
        """
        self.identifier = identifier
        self.command = command
        self.datasets = datasets if not datasets is None else dict()
        self.stdout = stdout if not stdout is None else list()
        self.stderr = stderr if not stderr is None else list()

    def copy(self):
        """Return a copy of the module handle.

        Returns
        -------
        vizier.workflow.module.ModuleHandle
        """
        # Return new module handle. Make sure to copy datasets and output lists.
        # The command specification is not copied because it is not meant to
        # updated at any point.
        return ModuleHandle(
            self.identifier,
            self.command,
            datasets=dict(self.datasets),
            stdout=list(self.stdout),
            stderr=list(self.stderr)
        )

    @staticmethod
    def from_dict(doc):
        """Create object instance from dictionary serialization.

        Parameters
        ----------
        doc : dict
            Dictionary serialization of the module handle

        Returns
        -------
        MongoDBModuleHandle
        """
        return ModuleHandle(
            doc['id'],
            ModuleSpecification.from_dict(doc['command']),
            {ds['name'] : ds['id'] for ds in doc['datasets']},
            doc['stdout'],
            doc['stderr']
        )

    @property
    def has_error(self):
        """Flag indicating whether there was an error during module execution.
        Currently, only the existience of output to STDERR is used as error
        indicator.

        Returns
        -------
        bool
        """
        return len(self.stderr) > 0

    def to_dict(self):
        """Get dictionary serialization of th #TXT_NORMAL, TXT_ERRORe module handle.

        Returns
        -------
        dict
        """
        return {
            'id' : self.identifier,
            'command' : self.command.to_dict(),
            'stdout' : self.stdout,
            'stderr': self.stderr,
            'datasets' : [{
                    'name' : key,
                    'id' : self.datasets[key]
                } for key in self.datasets
            ]
        }


class ModuleSpecification(object):
    """Specification of a workflow command that is added and evaluated as a
    module in a workflow. Contains the type, i.e., name of the interpreter, and
    a command-specific argument objects (as a dictionary).

    Attributes
    ----------
    module_type : string
        Name of the evaluation engine or package
    arguments : dict()
        Command-specific arguments
    """
    def __init__(self, module_type, command_identifier, arguments):
        """Initialize the module type, command identifier and command arguments.

        Parameters
        ----------
        module_type : string
            Name of the module type
        command_identifier : string
            Identifier of the module type specific command that is executed by
            thie module
        arguments : dict()
            Command-specific arguments
        """
        self.module_type = module_type
        self.command_identifier = command_identifier
        self.arguments = arguments

    @staticmethod
    def from_dict(doc):
        """Create module specification instance from dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of a module Specification

        Returns
        -------
        ModuleSpecification
        """
        return ModuleSpecification(
            doc['type'],
            doc['identifier'],
            doc['arguments']
        )

    def is_type(self, type_id):
        """Case-insensitive test if the module type matches a given value.

        Parameters
        ----------
        type_id : string
            String representing the command type

        Returns
        -------
        bool
        """
        return self.module_type.lower() == type_id.lower()

    def to_dict(self):
        """Get dictionary serialization for this module specification.

        Returns
        -------
        dict
        """
        return {
            'type': self.module_type,
            'identifier': self.command_identifier,
            'arguments': self.arguments
        }


class ModuleOutputs(object):
    """
    """
    def __init__(self):
        """Initialize the standard output and error stream."""
        self.std_out = list()
        self.std_err = list()

    def stderr(self, content=None):
        """Add content to the error output stream for a workflow module. Use
        to retrieve output stream when called without content parameter.

        Will raise ValueError if an invalid content object is given.

        Parameters
        ----------
        content: dict, optional
            Content object. Expected to contain type and data field.

        Returns
        -------
        list
        """
        # Validate content if given. Will raise ValueError if content is invalid
        if not content is None:
            self.validate_content(content)
            self.std_err.append(content)
        return self.std_err

    def stdout(self, content=None):
        """Add content to the regular output stream for a workflow module. Use
        to retrieve output stream when called without content parameter.

        Will raise ValueError if an invalid content object is given.

        Parameters
        ----------
        content: dict, optional
            Content object. Expected to contain type and data field.

        Returns
        -------
        list
        """
        # Validate content if given. Will raise ValueError if content is invalid
        if not content is None:
            self.validate_content(content)
            self.std_out.append(content)
        return self.std_out

    def validate_content(self, content):
        """Validate content dictionary to ensure that it contains type and
        data element.

        Raises ValueError if content is invalid.

        Parameters
        ----------
        content: dict, optional
            Content object. Expected to contain type and data field.
        """
        for key in ['type', 'data']:
            if not key in content:
                raise ValueError('missing key \'' + key + '\'')
        if len(content) > 2:
            for key in content:
                if not key in ['type', 'data']:
                    raise ValueError('invalid key \'' + key + '\'')

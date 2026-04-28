#!/usr/bin/env python3

"""
Custom exceptions for the updatecribllookup command/modular alert.
"""


class UpdateCriblLookupException(Exception):
    """Base exception class for updatecribllookup command errors."""
    pass


class WorkergroupNotProvidedException(UpdateCriblLookupException):
    """Exception raised when workergroup parameter is not provided."""
    

    def __init__(self, message="Workergroup parameter is required but was not provided"):
        self.message = message
        super().__init__(self.message)


class LookupNotProvidedException(UpdateCriblLookupException):
    """Exception raised when lookup parameter is not provided."""
    

    def __init__(self, message="Lookup parameter is required but was not provided"):
        self.message = message
        super().__init__(self.message)


class WorkergroupNotFoundException(UpdateCriblLookupException):
    """Exception raised when the specified workergroup is not found in configuration."""
    

    def __init__(self, workergroup_name=None):
        if workergroup_name:
            message = f"Workergroup '{workergroup_name}' not found in configuration"
        else:
            message = "Specified workergroup not found in configuration"
        self.message = message
        super().__init__(self.message)


class LookupNotFoundException(UpdateCriblLookupException):
    """Exception raised when the specified lookup is not found in configuration."""
    
    
    def __init__(self, lookup_name=None):
        if lookup_name:
            message = f"Lookup '{lookup_name}' not found in configuration"
        else:
            message = "Specified lookup not found in configuration"
        self.message = message
        super().__init__(self.message)


#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en' 

# wrapper class for all the errors encountered during user operation
class RuntimeProxyException (Exception):
	pass

# This exception is used when the directory structure passed to the functions is not the one we expect for the proxy system
class InvalidDirException (RuntimeProxyException):
	pass

# When the id of a shape has no match in our proxy listings
class InvalidShapeIdException (RuntimeProxyException):
	pass

# When the id of a proxy has no match in our proxy listings
class InvalidProxyException (RuntimeProxyException):
	pass

# When the id of a meta has no match in our proxy listings
class InvalidMetaException (RuntimeProxyException):
	pass

# When we cannot match the fs change event to the operative flow of the proxy
class InvalidFSOperationException (RuntimeProxyException):
	pass

class InvalidShapeArchiveException (RuntimeProxyException):
	pass


# wrapper class for failingsin the internal structure of the proxy (missing directories where they are expected to be etc)
# When these exceptions are encountered an administrator should stop the proxy operations and fix or rebuild its filesystem and configuration structures
class InternalProxyException (Exception):
	pass


# Informs that the required resource has already been unlocked. This should NEVER happen
class LockReleasedException (InternalProxyException):
	pass

# Used when we fail to release the lockfile (but may be because of a LockReleasedException) but we still have completed the action that locked the FS so we can return the value. Should be handled by logging withou blocking the rest of the process
class BadLockReleaseException (InternalProxyException):
	pass

# Informs that the required resource already has a lock file so it is not possible to set a new one. This is a NORMAL behaviour, not an error, and is used to make the process wait and retry the required operation
class ResourceLockedException (Exception):
	pass



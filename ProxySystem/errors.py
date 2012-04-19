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
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import zipfile

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

#from httplib import HTTPException
#from time import sleep
import sys
import os
#import os.path

from errors import *
import proxy_config_core as conf
import proxy_core

"""
This module is called when a change happens in the filesystem (specifically in the upload directory, but the proxy checks anyway in case the fs monitor cannot filter before informing the proxy)
"""

def handleFSChange (eventpath):
	"""
	Wrapper for the whole handleFileUpdate process.
	Launches handleFileUpdate and logs/informs about any and all breakages
	Note that all errors are caught as exceptions, not return codes.
	We expect that under normal operative conditions all operations on the proxy DO work properly AND silently.
	This function acts only as launcher and exception interceptor for logging, the actual controller is HandleFileUpdate()
	:param eventpath:
	:return:
	"""

	# we only do a quick check to ensure the file event is in our upload path

	eventpath = os.path.realpath(eventpath)
	uploadpath = os.path.realpath(conf.baseuploadpath)
	if not eventpath.startswith(uploadpath):
		# if the event is not in a subdir of $upload we simply ignore it and exit
		sys.exit(0)

	# otherwise we start the actual file update handling process
	try:
		handleFileEvent(eventpath)
	except:
		#TODO: add actual logging/mailing code
		pass



def handleFileEvent (eventpath):
	"""
	Acts as controller during the whole process of update (and eventual send in case of write/full
	:param eventpath: path to the changed/added/deleted file on the filesystem
	:return:
	"""

	# our upload dir structure is:
	# $upload / $proxy_instance / $meta_id / $shape_id.zip

	# we detect what has actually changed.
	# It MUST be a zip file or somebody is messing with the dir structure and we must exit and warn about it

	proxy_id, meta_id, shape_id = proxy_core.verifyUpdateStructure(eventpath)

	upsert = None
	# Determining if the event is an upsert or a delete
	if not os.path.exists(eventpath):
		# delete
		proxy_core.handleDelete (proxy_id, meta_id, shape_id)
	elif zipfile.is_zipfile(eventpath):
		# upsert
		proxy_core.handleUpsert (proxy_id, meta_id, shape_id)
		upsert = (shape_id,)
	else:
		# wrong file type or directory creation
		raise InvalidFSOperationException ("Unexpected file type or operation on path %s" % eventpath)

	#TODO: If the updating of the $mirror directory has  been completed properly (i.e. no exceptions), we rebuild the geoJSON for the specified meta

	if upsert is not None:
		shapedata = proxy_core.rebuildShape(proxy_id, meta_id, shape_id, modified=True)
		proxy_core.replicateShapeData (shapedata, proxy_id, meta_id, shape_id, modified=True)
	else:
		# this is a delete
		proxy_core.replicateDelete (proxy_id, meta_id, shape_id)

	proxy_core.queueForSend(proxy_id, meta_id)

	#TODO: if the server is a write/full server, we launch the server update process



















if __name__ == "__main__":
	handleFSChange (sys.argv[1])
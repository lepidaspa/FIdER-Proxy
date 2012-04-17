#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base_config

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

import sys
import os
import os.path


def handleFileUpdate (eventpath):
	"""
	Receives a filesystem event notification and launches the correct course of action (delete or update)
	:param eventpath:
	:return:
	"""

	# CHECK that the event has not been created by the proxy itself (i.e. is NOT in the baseproxy directory)

	filename, path = os.path.realpath(eventpath).split()

	# checking if the path MAY be the compatible with the proxy upload path
	try:
		uploadpath, tokenpath, metapath = path[0:-3], path[-2], path[-1]
	except:
		sys.exit(0)


	# CHECK that the event is in the uploadpath (otherwise we don't care, just in case)

	proxyuploadpath = os.path.realpath(base_config.baseuploadpath)

	# checking if we are actually in the upload path of the proxy
	if proxyuploadpath != uploadpath:
		sys.exit(0)


	# .1 check if the token refers to a valid proxy

	softproxypath = os.path.join (base_config.baseproxypath, tokenpath)
	if not os.path.isdir(softproxypath):
		raise Exception ("Non valid proxy id: %s" % tokenpath)

	# .2 check if the meta_id refers to a valid metadata inside this proxy
	softmetapath = os.path.join(softproxypath, "conf", "mappings", metapath)
	if not os.path.isdir(softmetapath):
		raise Exception ("Non valid metadata id for proxy %s: %s" % tokenpath, metapath)


	import hardproxy

	# CHECK if the request path is an existing file: true = update, false = delete

	if os.path.isfile(eventpath):
		# receive as upsert
		hardproxy.receiveShapefileUpdate(eventpath)

	elif not os.path.exists(eventpath):
		#receive as delete
		hardproxy.receiveShapefileDelete(eventpath)

	else:
		raise Exception ("Unexpected file status/operation on path %s" % eventpath)


#TODO: add general system handler to capture ALL exceptions and send mail

if __name__ == "__main__":
	handleFileUpdate(sys.argv[1])
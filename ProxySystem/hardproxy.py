#!/usr/bin/env python
# -*- coding: utf-8 -*-
import httplib
import json
import shutil
import stat
import urllib
import zipfile
import message_templates

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

import os
from osgeo import ogr

import base_config

#TODO: add new pre-manifest creation

def buildMainProxy ():
	"""
	Creates the basic proxy structure according to the settings specified in the configuration file and copies the main server data in the datapath so that it can be changed simply by replacing a non-Python file
	:return:
	"""

	# Creating the main data directory
	os.makedirs(os.path.join(base_config.baseproxypath))
	# Creating the upload directory
	os.makedirs(os.path.join(base_config.baseuploadpath))

	os.makedirs(os.path.join(base_config.baseproxypath, "log"))
	os.makedirs(os.path.join(base_config.baseproxypath, "locks"))

	# copying the server information file in the data directory of the proxy
	shutil.copyfile (base_config.mainserver_ref_location, os.path.join(base_config.baseproxypath,os.path.split(base_config.mainserver_ref_location)[1]))



def getManifest (proxyinstance):
	"""
	Retrieves the manifest of a soft proxy according to its unique token and returns it as a dict
	:param proxyinstance:
	:return:
	"""

	manifestfilename = "manifest.json"
	manifestpath = os.path.join(base_config.baseproxypath, proxyinstance, 'conf', manifestfilename)

	fp = open(manifestpath, 'r')
	jsondata = json.load(fp)
	fp.close()

	return jsondata





def sendManifest (proxyinstance):
	"""
	Sends a chosen manifest file to the main server as RESPONSE to its GET for capabilities
	:param proxyinstance:
	:return:
	"""

	#TODO: empty -> implement sendManifest function
	#NOTE: we need the structure of the main server info file



def buildProxyInstance (premanifest, servertoken):
	"""
	With a pre-set manifest, this function creates the filesystem structure for a new "soft" proxy instance according to the general config file.
	:param premanifest:
	:param servertoken:
	:return:
	"""

	#TESTING NOTES: the pre-manifest is taken from the config_testing placeholder file.

	basepath = base_config.baseproxypath

	#NOTE: we assume that the basic directories for the hard proxy ($UPLOAD and $BASEPATH) ALREADY exist

	#getting list of all metadata names
	meta_ids = []
	for i in range (0, len(premanifest['metadata'])):
		meta_ids.append(premanifest['metadata'][i]['name'])

	# 1. Creating the first level under the base proxy dir. We do not create locks/ and log/ dirs as they should be already there

	proxypath = os.path.join(basepath, servertoken)
	os.makedirs (proxypath)

	# 2. Creating the lower branches

	for meta_id in meta_ids:

		# 2. Creating the mappings dir for each metadata
		os.makedirs (os.path.join(proxypath, 'conf', 'mappings', meta_id))

		# 3.1 Creating the mirror directory for the raw shape files
		os.makedirs (os.path.join(proxypath, 'maps', 'mirrors', meta_id))
		# 3.2 Creating the storage directory for the geoJSON files
		os.makedirs (os.path.join(proxypath, 'maps', 'gjs', meta_id))

		# 3.3 Creating the next-changes directory
		os.makedirs (os.path.join(proxypath, 'next'))

		# 3. Creating the Upload directories
		# (kept here only to save a loop, logically it should be out)

		os.makedirs (os.path.join(base_config.baseuploadpath, servertoken, meta_ids))


	# 4. Creating the actual manifest file

	premanifest['token'] = unicode(servertoken)
	premanifest['message_type'] = u'response'
	premanifest['message_format'] = u'capabilities'

	# 5. Export the manifest to its intended location

	manifest_path = os.path.join(proxypath, 'conf', servertoken+".manifest")

	manifest_filepointer = open(manifest_path, 'w')
	json.dump(manifest_filepointer)
	manifest_filepointer.close()

	# 6. Send manifest back to main server

	sendManifest(manifest_path)



def convertShapeFileToJson (filepath):
	"""
	Converts a Shape file (note: group of files, actually) from a given path to a GeoJSON file
	:param filepath:
	:return:
	"""


	container = {
		'type': 'FeatureCollection',
		'features' : []
	}

	try:
		datasource = ogr.Open(filepath)
	except:
		return False

	jsonlist = []

	for i in range (0, datasource.GetLayerCount()):
		layer = datasource.GetLayer(i)
		for f in range (0, layer.GetFeatureCount()):
			feature = layer.GetFeature(f)
			jsondata = feature.ExportToJson()

			container['features'].append(jsondata)

	return container

def SaveJsonFile (sourcepath, jsondata):
	"""
	Takes a geojson object and saves it to the appropriate meta file
	:param sourcepath:
	:param jsondata:
	:return:
	"""

	spath = sourcepath.split("/")
	shapename, meta_id, proxyinstance = spath[-1], spath[-2], spath[-3]

	destpath = os.path.join(base_config.baseproxypath, proxyinstance, "maps", "gjs", meta_id, shapename[:-4]+".json")

	fp = os.open(destpath, 'w')
	json.dump(jsondata, fp)
	fp.close()


def createLock (proxyinstance, meta_id, reason=None):
	"""
	Creates a lock file in the lock directory of the proxy. The lock file is write-protected and contains, if applicable, the specific reason for locking (i.e. conversion underway, uploading to server, etc)
	:param proxyinstance: string, unique token of the softproxy
	:param meta_id: string, name of the metadata on which we are working
	:param reason: string
	:return:
	"""

	lockpath = (os.path.join(base_config.baseproxypath, "locks", proxyinstance+"."+meta_id))
	if os.path.exists(lockpath):
		raise Exception ("lockfile for %s/%s already exists" % (proxyinstance,meta_id))
	fp = open(lockpath, 'w')
	if not reason is None:
		fp.write(reason)

	fp.close()

	# setting to read-only
	os.chmod(lockpath, stat.S_IREAD)

def removeLock (proxyinstance, meta_id):
	"""
	Removes the chosen lock file.
	:param proxyinstance: string, unique token of the softproxy
	:param meta_id:
	:return:
	"""

	lockpath = (os.path.join(base_config.baseproxypath, "locks", proxyinstance+"."+meta_id))
	#unlocking
	os.chmod(lockpath, stat.S_IWRITE)
	#removing
	if not os.path.exists(lockpath):
		raise Exception ("tried to remove non-existing logfile for %s/%s" % (proxyinstance, meta_id))

	os.remove(lockpath)



def addToChangesList (proxyinstance, meta_id):
	"""
	Adds a metadata to the list of updated files for this proxy
	:param proxyinstance:
	:param meta_id:
	:return:
	"""

	nextfilepath = os.path.join (base_config.baseproxypath, proxyinstance, "next", meta_id)

	open(nextfilepath, 'w').close()

def receiveShapefileDelete (zipfilepath):
	"""
	Receives a filepath of a deleted file, cleans up the storage and geojson directories accordingly. Does not use the lock since we do not perform any modification on the data, so if anything is added we can simply overwrite the current change
	:param zipfilepath:
	:return:
	"""

	#1. Go to the GeoJSON directory
	zpath = zipfilepath.split("/")
	zipfilename, meta_id, proxyinstance = zpath[-1], zpath[-2], zpath[-3]
	shapename = zipfilename[:-4]

	gjdir = os.path.join(base_config.baseproxypath, proxyinstance, "maps", "gjs", meta_id, shapename)
	shapedir = os.path.join (base_config.baseproxypath, proxyinstance, "maps", "mirror", meta_id, shapename)

	#1.1 Remove all in the directory (but not the directory itself)

	for disposable in os.listdir(gjdir):
		os.remove (os.path.join(gjdir, disposable))

	for disposable in os.listdir(shapedir):
		os.remove (os.path.join(shapedir, disposable))

	#2. add to the list of file changes
	addToChangesList(proxyinstance, meta_id)



def receiveShapefileUpdate (zipfilepath):
	"""
	Inspects the latest shapefile received (as a zip file in the upload folder) and updates the soft proxy data directories accordingly. Does not include the push towards the main server. This should be launched by a script triggered by the file modifications in a certain range of paths. Path of the candidate zipfile MUST be absolute

	The zip file is taken from the upload directory and unpacked in a subdirectory of the mirror area, then the data is converted to the GeoJSON format and saved in the gjs directory. Note that this process happens for each shapefile that is uploaded. Also, the system expects the archive to have the name of the pertinent shapefile inside it, so $NAME.zip will cause the script to look for $NAME.shp etc.
	As the function is called from another script, any error is passed straight with its  exception data for proper handling.

	:param zipfilepath: path to the zipfile that has been updated
	:return:
	"""



	# 1. Loading the zipfile

	zpath = zipfilepath.split("/")

	zipfilename, meta_id, proxyinstance = zpath[-1], zpath[-2], zpath[-3]


	createLock (proxyinstance, meta_id, "Converting %s into geojson data" % zipfilepath)

	#removing the .zip extension, anything else should be considered the name of the shapefile
	shapename = zipfilename[:-4]


	zipfp = zipfile.ZipFile(zipfilename, mode='r')


	# 1.1 Verifying the archive: no file should have a path, all should have the same name (not extension) being the same as the one of the archive itself.
	zipped = zipfp.namelist()
	mandatory = {
		".shp": False,
		".shx": False,
		".dbf": False
	}

	valid_extensions = [".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", ".fbx", ".ain", ".aih", ".ixs", ".mxs", ".atx", ".cpg", ".shp.xml"]
	#NOTE: extensions shp.xml is handled separately

	for filename in zipped:
		# basic filename check
		if filename[:-4] != shapename:
			raise Exception ("Inconsistent file naming in shape archive, %s vs %s" % (filename, zipfilename))



		#extensions check

		hasvalidextension = False
		for extension in valid_extensions:
			if filename.endswith (extension):
				hasvalidextension = True
			if extension in mandatory.keys():
				mandatory[extension] = True

		if hasvalidextension is False:
			raise Exception ("Non valid shapefile component extension in file %s" % filename)

		for key in mandatory:
			if mandatory[key] is False:
				raise Exception ("Missing mandatory file (%s)" % key)

	# 2. Unpack to other dir


	destpath = os.path.join(base_config.baseproxypath, proxyinstance, base_config.dir_mirror, meta_id, shapename)

	#2.1 cleaning up dest directory, if exists; if the directory does exist but we get an error, we send the exception up. Should not handle non accessible filesystem

	if os.path.exists(destpath):
		shutil.rmtree(destpath)
	os.makedirs (destpath)

	#2.2 actual extraction
	zipfp.extractall(destpath)

	# 3. Translate to GeoJSON
	geojson = convertShapeFileToJson(os.path.join(destpath,shapename+".shp"))

	gjdestpath = os.path.join(base_config.baseproxypath, proxyinstance, base_config.dir_geojson, meta_id, shapename, shapename+".geojson")


	geojson_filepointer = open(gjdestpath, 'w')
	json.dump(geojson, geojson_filepointer)
	geojson_filepointer.close()


	removeLock (proxyinstance, meta_id)
	#TODO: add logging

	#2. add to the list of file changes
	addToChangesList(proxyinstance, meta_id)
	# we always use this list even if on write full so we can be sure


	#If proxy is on full write, we send the data at once
	#Otherwise a different script will launch the sync later
	manifestdata = getManifest(proxyinstance)
	if manifestdata['operations']['write'] == 'full':
		sendWriteRequest (proxyinstance)


def sendWriteRequest (proxyinstance, fullsync=False):
	"""
	Sends the current updates from the chosen softproxy to the main server
	:param proxyinstance:
	:param fullsync: boolean, tells whether we want to send everything or only updated data; this is not the same as the mode in the proxy manifest since the mode describes WHEN the update is launched, which is outside the scope of this function
	:return:
	"""

	if fullsync is not True:
		#NOTE: full or sync is the same to us in terms of chosen objects, the path for the updates is the same in both modes, so we can hardcode it
		scope = 'full'
	else:
		scope = 'restore'

	upserts = buildUpsertsList(proxyinstance, ('write', scope))


	#NOTE: this function in itself does NOT create the lockfiles since they should have been created by the createChangesList function via the buildUpsertsList function. However, we CHECK that we have locked everything and we stop operating at once if any lockfile is missing because we could be losing consistency (technically consistency is lost when the file is altered, which is as soon as something else puts a lock on it for writing, which should not be possible, but the one condition we can easily verify is the lack of lockfile)
	#TODO: create smarter content for the lockfile so that we can check if the lockfile, even standing there, is there for the sending process and not for rewriting. Can be done simply at the moment by putting specific messages in the lockfile itself.

	for meta_id in upserts:
		if not os.path.exists(base_config.baseproxypath, "locks", proxyinstance+"."+meta_id):
			raise Exception ("Lock for proxy.map %s.%s has been removed" % (proxyinstance, meta_id))



	#TODO: Create the message as a request template, send and get answer from main server. If the write succeeds, remove all the pertinent locks in the lock directory. This happens also if the send fails, since we cannot block changes if the next send is very far away in time

	msg = message_templates.template_request_write

	msg['token'] = proxyinstance
	msg['upserts'] = upserts


	#TODO: implement "TRY TO SEND" mechanic

	response = sendRequestToServer(msg, 'POST')


	# after trying to send
	for meta_id in upserts:
		removeLock(proxyinstance, meta_id)


def getMainServerConfig ():
	"""
	checks the configuration file to find the main host settings file in the proxy path and retrieves the key/value pairs from it
	:return:
	"""

	serverconfraw = file ( os.path.join(base_config.baseproxypath, base_config.mainserver_ref_location) )

	serverconf = {}
	for line in serverconf:
		key, val = line.strip().split("=", 1)
		serverconf[key] = val




def sendRequestToServer (jsonmess, method="POST"):
	"""
	Sends a json message to the main server according to the method requested and returns the answer from the server. The result should always be a json message if the interaction works correctly. All the data for proxy identification is included in the json data
	:param jsonmess:
	:param method: string, GET/POST
	:return:
	"""


	conn_complete = False
	tries = 0
	while (not conn_complete) and tries < base_config.tries_for_connection:

		tries += 1


		serverdata = getMainServerConfig()

		conn = httplib.HTTPSConnection(serverdata['federatore_host'])
		headers = {"Content-type": "application/json",
				   "Accept": "text/plain"}
		params = urllib.urlencode(jsonmess)
		conn.request("POST", "", params, headers)
		result = conn.getresponse()
		conn.close()

		#TODO: replace placeholder with actual check on the result/response
		#currently we leave automatically after the first try
		if True:
			conn_complete = True

	#TODO: handle various results, determine if the result is ok or causes an error in the soft proxy or if we retry, currently we have a placeholder for "ALL OK"

	#TODO: use anomalies to mark only SOME lock files

	#Interaction completed specifies if the interaction has been "logically ok". A response with anomalies or an impossibility to connect are equally fine with this, we just want to know that there are no inconsistencies or other major issues that mean we should NOT remove the lock files. We can potentially add a new reason to the lock files so that we know why the lock cannot be removed on request from the main server
	interaction_completed = True

	if interaction_completed:
		#remove all lock from the list in the json file
		if jsonmess.has_key('acknowledge') and jsonmess['acknowledge'].has_key('upserts'):
			for meta_id in jsonmess['acknowledge']['upserts'].keys():
				removeLock(jsonmess['token'], meta_id )



def buildUpsertsList (proxyinstance, sendmode=None):
	"""
	Send the current changes
	:param proxyinstance:
	:param sendmode: tuple of two strings, operation(read/write) and mode (full, sync/diff, none); this is used only to avoid reloading the manifest in case we just opened it; also, it allows for forced syncs when needed
	:return:
	"""

	if sendmode is None:
		sendmode = [None, None]
		manifestdata = getManifest(proxyinstance)
		# IMPORTANT: a manifest can only have ONE sending mode (read or write) when this function is used, it's not called for query mode
		if manifestdata['operations']['read'] != 'none':
			sendmode[0] = 'read'
			sendmode[1] = manifestdata['operations']['read']
		elif manifestdata['operations']['write'] != 'none':
			sendmode[0] = 'write'
			sendmode[1] = manifestdata['operations']['write']
		else:
			# MANIFEST IS BROKEN! Leaving now
			raise ValueError ('Manifest for %s does not have a valid operations value, either write or read should be different than \'none\'' % proxyinstance)

	#NOTE: restore is not a flag in the specifications since it is only to be used on a needs-to basis. It is the only way to force the system to send a write request with all the maps on a certain proxy
	if sendmode[0] == 'read' and sendmode[1] is 'full':
		fullsync = True
	elif sendmode[0] == 'write' and sendmode[1] is 'restore':
		fullsync = True



	return createChangesList (proxyinstance, fullsync, lockdata=True)



def createChangesList (proxyinstance, fullsync=False, lockdata=False):
	"""
	Gets the list of updated elements from the "next" directory of the softproxy and sends a json message to the main server with the list of materials that have been updated since the last send.
	:param proxyinstance: token used by the main server to identify the specific soft proxy
	:param fullsync: boolean, if we are syncing the whole map list or only the updated meta
	:param lockdata: boolean, if we are using this to send then we want to lock the files until everything is completed
	:return: tuple with lists of upserts and deletes
	"""

	#TODO: Add locks for each meta with cause of sending, since this is used when we are sending data to the main server

	#1 we get the list of updated metadata
	if not fullsync:
		nextpath = os.path.join (base_config.baseproxypath, proxyinstance, "next")
	else:
		nextpath = os.path.join (base_config.baseproxypath, proxyinstance, "maps", "gjs")


	changes = os.listdir(nextpath)


	#Creating the upserts list as a dictionary, each meta will use its own name as the key for the list of its own objects
	changeslist = {}

	for changeloc in changes:
		#2 we find the metadata dir and send everything up
		changepath = os.path.join (base_config.baseproxypath, proxyinstance, "maps", "gjs", changeloc)
		# we have found a meta, so we need to get all the .json files under it

		fullmetalist = os.listdir(changepath)

		#Creating the meta as a list of geographical objects
		objectslist = []

		for filename in fullmetalist:
			detailpath = os.path.join(changepath, filename)
			fp = open(detailpath)
			jsondata = json.load(fp)
			fp.close()
			objectslist.append(jsondata)

		changeslist[changeloc] = (objectslist)

	return changeslist





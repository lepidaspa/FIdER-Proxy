#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from osgeo import ogr
import shutil
import zipfile

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

import os.path
import os

import proxy_config_core as conf
from errors import *

def verifyUpdateStructure (eventpath):
	"""
	Takes the path of the file event and returns, if verified, the identification of proxy, metadata and shape
	:param eventpath:
	:return: list with proxy_instance, meta_id, shape_id
	"""


	fullpath = eventpath.split("/")

	basepath, proxy_id, meta_id, zipfilename = eventpath.split()

	if os.path.realpath(basepath) != os.path.realpath(conf.baseuploadpath):
		raise InvalidDirException ("Upload path structure %s is not matched by event path %s" % (conf.baseuploadpath, eventpath))

	# getting the shape_id by removing the .zip extension from the zipfile name
	try:
		shape_id = zipfilename[:-4]
	except:
		raise InvalidShapeIdException ("Could not extract a valid shapeid from the shape file archive name" % zipfilename)

	# getting manifest data for verification

	try:
		manifest_fp = open(os.path.join(conf.baseproxypath, proxy_id, conf.path_manifest))
	except:
		raise InvalidProxyException ("Proxy instance %s does not exist" % proxy_id)
	else:
		manifest = json.load(manifest_fp)
		manifest_fp.close()

	# checking the meta_id against the list
	meta_in_manifest = False
	for currentmeta in manifest['metadata']:
		if currentmeta['name'] == meta_id:
			meta_in_manifest = True
			break

	if not meta_in_manifest:
		raise InvalidMetaException ("Could not find meta_id %s in proxy %s" % (meta_id, proxy_id))

	return proxy_id, meta_id, shape_id


def handleDelete (proxy_id, meta_id, shape_id):
	"""
	Removes the shapefile data from the $mirror directory
	:param proxy_id:
	:param meta_id:
	:param shape_id:
	:return:
	"""

	path_mirror = os.path.join(conf.baseproxypath, proxy_id, conf.path_mirror, meta_id, shape_id)

	if not os.path.exists(path_mirror):
		raise Exception ("Data for %s/%s already deleted in the mirror section of proxy %s" % (meta_id, shape_id, proxy_id))
	else:
		#TODO: add specific handling of further exceptions or just push it up the ladder
		shutil.rmtree(path_mirror)

def replicateDelete (proxy_id, meta_id, shape_id):
	"""
	Removes the shapefile data from the gjs directory
	:param proxy_id:
	:param meta_id:
	:param shape_id:
	:return:
	"""

	path_gj = os.path.join(conf.baseproxypath, proxy_id, conf.path_geojson, meta_id, shape_id)

	if not os.path.exists(path_gj):
		raise Exception ("Data for %s/%s already deleted in the geojson section of proxy %s" % (meta_id, shape_id, proxy_id))
	else:
		#TODO: add specific handling of further exceptions or just push it up the ladder
		shutil.rmtree(path_gj)

def handleUpsert (proxy_id, meta_id, shape_id):
	"""
	This function adds or modifies the shapefile data to the $mirror directory
	:param proxy_id:
	:param meta_id:
	:param shape_id:
	:return:
	"""

	# first we check if the directory already exists
	# in case we remove it and write the new data so we ensure we use a clean environment

	try:
		zipfilename = os.path.join(conf.baseuploadpath, proxy_id, meta_id, shape_id, ".zip")
		zipfp = zipfile.ZipFile(zipfilename, mode='r')
	except:
		#leaving as placeholder in case we want to add a more specific handling
		raise

	# Inspecting zip file to ensure it does NOT contain path data in the filenames, which is forbidden in our use, and that the files in it have the proper naming for a SINGLE shapefile structure

	ext_mandatory = {
		".shp": False,
		".shx": False,
		".dbf": False
	}

	ext_accept = [
		".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", ".fbx", ".ain", ".aih", ".ixs", ".mxs", ".atx", ".cpg", ".shp.xml"
	]

	for candidatepath in zipfp.namelist():
		#checking that no file unpacks to a different directory
		if "/" in candidatepath:
			raise InvalidShapeArchiveException ("Shapefile archives should not contain names with path data")
		else:
			#checking that the names of the file are correct
			cext = None
			for valid in ext_accept:
				if candidatepath.endswith(valid):
					cext = valid
					if cext in ext_mandatory:
						ext_mandatory[cext] = True
					break
			if cext is None:
				raise InvalidShapeArchiveException ("Shape archive %s contains unrelated data in file %s " % (shape_id, candidatepath))

	if not all(ext_mandatory.values):
		raise InvalidShapeArchiveException ("Mandatory file missing in shape archive %s (should contain .shp, .shx and .dbf)" % shape_id)

	#creating the path after opening the zip so there is a smaller risk of leaving trash behind if we get an error
	path_mirror = os.path.join(conf.baseproxypath, proxy_id, conf.path_mirror, meta_id, shape_id, ".tmp")
	if os.path.exists(path_mirror):
		shutil.rmtree(path_mirror)
	os.makedirs(path_mirror)

	#TODO: ensure that we remove any read-only flags and set the correct permissions if needed
	zipfp.extractall(path_mirror)


def convertShapefileToJson (path_shape):
	"""
	Converts a shapefile to GeoJSON data and returns it.
	:param path_shape: path of the shape file to be converted
	:return: geojson feature data
	"""

	collection = {
		'type': 'FeatureCollection',
		'features' : []
	}

	try:
		datasource = ogr.Open(path_shape)
	except:
		return False

	jsonlist = []

	for i in range (0, datasource.GetLayerCount()):
		layer = datasource.GetLayer(i)
		for f in range (0, layer.GetFeatureCount()):
			feature = layer.GetFeature(f)
			jsondata = feature.ExportToJson()

			collection['features'].append(jsondata)

	return collection


def rebuildShape (proxy_id, meta_id, shape_id, modified=True):
	"""
	Rebuilds the GeoJSON data for the specified shape file, from the .tmp subdir if the file is marked as modified. Returns the geojson dict
	:param proxy_id:
	:param meta_id:
	:param shape_id:
	:param modified:
	:return: dict, geojson data
	"""


	path_shape = os.path.join(conf.baseproxypath, proxy_id, conf.path_mirror, meta_id, shape_id)
	if modified:
		path_shape = os.path.join(path_shape, ".tmp")

	shape_gj = convertShapefileToJson (path_shape)

	return shape_gj

def replicateShapeData (shapedata, proxy_id, meta_id, shape_id, modified=True):
	"""
	Saves the current geojson data for a specific shape to the geojson directory. If modified is true, the .tmp directory in the mirror section replaces the old data
	:param shapedata:
	:param proxy_id:
	:param meta_id:
	:param shape_id:
	:return:
	"""

	try:
		shape_fp = open (os.path.join(conf.baseproxypath, proxy_id, conf.path_geojson, meta_id, shape_id))
		json.dump(shapedata, shape_fp)
		shape_fp.close()
	except:
		#TODO: add more complex exception handling
		raise

	if modified:
		# we replace the mirror directory contents with the .tmp directory
		path_mirror = os.path.join(conf.baseproxypath, proxy_id, conf.path_mirror, meta_id, shape_id)
		path_mirror_tmp = os.path.join(path_mirror, ".tmp")


		# TODO: add handling for remove errors, should we have any "strays"
		for filename in os.listdir(path_mirror):
			if filename != ".tmp":
				os.remove(os.path.join(path_mirror, filename))

		# TODO: add handling for remove errors to avoid data losses
		for filename in os.listdir(path_mirror_tmp):
			shutil.copy(os.path.join(path_mirror_tmp, filename), path_mirror)

		shutil.rmtree (path_mirror_tmp)

def rebuildMeta (proxy_id, meta_id, upserts=None):
	"""
	Rebuilds the GeoJSON data for the specified meta, taking the requested upserts from their .tmp dirs instead. Note that the data has been already partially validated and extracted
	:param proxy_id:
	:param meta_id:
	:param upserts: list with the elements in the meta that must be taken from their own .tmp dir rather than from the main $mirror branch
	:return: dict of geojson elements, with shape_ids as key
	"""

	path_meta = os.path.join(conf.baseproxypath, proxy_id, conf.path_mirror, meta_id)

	shapelist = os.listdir(path_meta)

	shapes_gj = {}
	for shape_id in shapelist:
		if shape_id in upserts:
			modified = True
		else:
			modified = False
		shapes_gj [shape_id] = rebuildShape(proxy_id, meta_id, shape_id, modified)

	return shapes_gj

def queueForSend (proxy_id, meta_id):
	"""
	Adds a metadata to the list of updated files for this proxy
	:param proxy_id:
	:param meta_id:
	:return:
	"""

	nextfilepath = os.path.join (conf.baseproxypath, proxy_id, "next", meta_id)

	open(nextfilepath, 'w').close()














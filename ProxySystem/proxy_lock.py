#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import os.path

from errors import *
import proxy_config_core as conf

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'


def fuseParams (source_function, source_frame, names=None):
	"""
	Returns a dict with all the values indicated in list names passed as parameters to the source_f by the current frame. If names is None, then all key/values are returned
	:param names: list of strings
	:param source_function: function
	:param source_frame: frame in the python stack
	:return: dict
	"""

	spec = inspect.getargspec(source_function)
	vals = inspect.getargvalues(source_frame)

	fusion = {}

	for i in range (0, len(spec.args)):
		#we iterate in the list of arguments used by source_function

		kwoffset = -1
		# first we look if the arg we are checking is handled positionally
		try:
			fusion[spec.args[i]] = vals.locals['args'][i]
		except:
			kwoffset += 1
			#otherwise the arg is either passed as a keyword argument OR has a default value from source_function
			if spec.args[i] in vals.locals['kwargs']:
				#if the arg is passed as a keyword
				fusion[spec.args[i]] = vals.locals['kwargs'][spec.args[i]]
			else:
				#otherwise we fallback to the default value found in the function declaration
				fusion[spec.args[i]] = spec.defaults[kwoffset]

	return fusion


def setFSLock (proxy_id, meta_id, shape_id = None):

	if shape_id is not None:
		lockname = meta_id+"."+shape_id
	else:
		lockname = meta_id

	lockdir = os.path.join (conf.baseproxypath, proxy_id)
	lockpath = os.path.join (lockdir, lockname)

	locklist = os.listdir(lockdir)

	#first we check to see if there is a lock on the whole meta
	if (lockfile.startswith(meta_id) for lockfile in locklist):
		raise ResourceLockedException ("Meta %s is currently locked" % meta_id)

	#if not, we check to see if there is a lock on the specific shape file, or on ANY file in case we are locking the whole meta
	if shape_id is not None:
		# searching for a specific lock file on shape data
		if lockname in locklist:
			raise ResourceLockedException ("Resource %s is currently locked" % lockname)
	else:
		# searching for ANY lock file on the meta
		if any (lockfile.startswith(lockname+".") for lockfile in locklist):
			raise ResourceLockedException ("At least one resource of meta %s is currently locked" % meta_id)

	try:
		os.open(lockpath, 'w').close()
	except:
		raise InternalProxyException ("Could not create lockfile %s" % lockpath)


	return True



def releaseFSLock (proxy_id, meta_id, shape_id = None):

	if shape_id is not None:
		lockname = meta_id+"."+shape_id
	else:
		lockname = meta_id

	lockdir = os.path.join (conf.baseproxypath, proxy_id)
	lockpath = os.path.join (lockdir, lockname)

	locklist = os.listdir(lockdir)

	# NOTE: Currently we do not check if sub-locks have been made for a meta-level lock

	try:
		os.remove(lockpath)
	except:
		if os.path.exists(lockpath):
			# lock exists but we cannot remove it
			raise InternalProxyException ("Could not delete existing file %s" % lockpath)
		elif os.path.exists(lockdir):
			# lock does not exist but its directory does
			raise LockReleasedException ("Lockfile %s has been already deleted" % lockpath)
		else:
			# the lock directory itself does not exist any more
			raise InternalProxyException ("Could not find lock directory for %s/%s" % (proxy_id, meta_id))


	return True




class ProxyLocker ():
	"""
	Creates a locker object that builds and removes lockfiles around specific action contexts as required. Can have a default selection of proxy, meta and shape in case the functions don't pass them explicitly
	"""

	def __init__ (self, proxy_id=None, meta_id=None, shape_id=None, retries=0, wait=0):

		self.proxy_id = proxy_id
		self.meta_id = meta_id
		self.shape_id = shape_id

		if not isinstance(retries, int) or retries < 0:
			retries = 0

		if not isinstance(wait, int):
			wait = 0

		#Note that the tries mechanism applies only to setting the lockfile, after that we expect to have full control on the areas we are writing on

		self.tries = retries+1
		self.wait = wait




	def performLocked (self, action, *args, **kwargs):
		"""
		Performs the function named by the action parameter. Returns the same as the intended action or an Exception
		:param action: callback function
		:param args: positional parameters for the action
		:param kwargs: named parameters for the action
		:return: whatever returns the original function
		"""

		# we get the custom information on what really needs to be locked
		# first we extract everything with inspection

		# 0. PREPARATION

		args_vals = fuseParams (action, inspect.currentframe())

		try:
			shape_id = args_vals['shape_id']
		except:
			shape_id = self.shape_id

		try:
			meta_id = args_vals['meta_id']
		except:
			meta_id = self.meta_id

		try:
			proxy_id = args_vals['proxy_id']
		except:
			proxy_id = self.proxy_id

		if proxy_id is None or meta_id is None:
			raise RuntimeProxyException ("Cannot create lock file for unknown proxy or  meta")


		# 1. CREATING THE LOCK FILE

		attempt = 0
		issue = None
		success = False
		while attempt < self.tries:
			try:
				success = setFSLock (proxy_id, meta_id, shape_id)
			except Exception as ex:
				issue = ex

		if success is not True:
			#NOTE: check Python docs, a generic raise may be enough
			raise issue

		# 2. Perform the required operation

		try:
			output = action(*args, **kwargs)
		except Exception as ex:
			issue = ex

			# we try to release the lockfile and THEN we send the core exception up
			try:
				releaseFSLock(proxy_id, meta_id, shape_id)
			except Exception as ex:
				raise InternalProxyException ("Failed to release lockfile for %s/%s.%s \n(%s)\n while closing after the following error:\n%s", (proxy_id, meta_id, shape_id, ex.message, issue.message))


			raise issue


		# 3. Release the lockfile if we did NOT crash the main action
		try:
			releaseFSLock(proxy_id, meta_id, shape_id)
		except Exception as ex:
			raise BadLockReleaseException ("Failed to release lockfile for %s/%s.%s \n(%s)\n after successful activity ", (proxy_id, meta_id, shape_id, ex.message))
		#TODO: CHECK the actuall effect of raise+finally
		# it may be ok as long as the top process uses the exception ONLY by lodding
		finally:
			return output









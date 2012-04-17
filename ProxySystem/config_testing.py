#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

HARDPROXY_DATAPATH = "./tests/hpinstance/"
UPLOADPATH = "./tests/uploads/"

PROXYADMIN_MAIL = "avaccarino@labs.it"

METADATA_FAKE = {
	'name' : 'mapfile01',
	'desc' : 'default testing map',
	'time' : [u'2012-04-12T15:48Z', u'2012-04-12T15:48Z'],
	'area': [-180, -90, 180, 90]
}

#does NOT include token because the token is passed by the main server, nor the message_type and message_format fields that are added on the fly for the sending only
MANIFEST_PRE = {
	'base_url': u'http://localhost/',
	'area': [-180, -90, 180, 90],
	'time': [u'2012-04-12T15:48Z', u'2012-04-12T15:48Z'],
	'operations': {
		'read': u'full',
		'write': u'none',
		'query': {
			'inventory': u'none',
			'geographic': u'none',
			'time': u'none',
			'bi': u'none'
		}
	},
	'signs': False,
	'metadata': [METADATA_FAKE,]
}

MAINSERVER_CONF_FILE = 'federatore.info'
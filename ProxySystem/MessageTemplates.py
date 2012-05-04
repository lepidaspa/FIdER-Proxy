#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'


#Modified from the Message Validator package, models are ArDiVa-compliant

model_response_capabilities = {
	"token": unicode,
	"message_type": (u'response',),
	"message_format": (u'capabilities',),
	"base_url": unicode,
	"area": list,
	"time": list,
	"operations": {
		"write": (u"full", u"sync", u"none"),
		"read": (u"full", u"diff", u"none"),
		"query": {
			"inventory": (u"full", u"simple", u"none"),
			"geographic": (u"full", u"BB", u"none"),
			"time": (u"full", u"none"),
			"bi": (u"full", u"simple", u"none")
		},
		"signs": bool,
		"metadata": list
	}
}

model_response_welcome = {
	"token": unicode,
	"message_type": (u'response',),
	"message_format": (u'welcome',),
	"latest_model_version": unicode,
}

model_field_anomaly = {
	"id": unicode,
	"anomaly": unicode,
	"BB": list
}

#In response_read and request_write the upserts list is actually a dict with keys the meta_ids and values the list of feature_collections for such meta_ids

model_response_read = {
	"token": unicode,
	"message_type": (u'response',),
	"message_format": (u'read',),
	"operation": (u'full', u'diff', u'none'),
	"data": {
		"upsert": dict,
		"delete": list
	}
}

model_request_write = {
	"token": unicode,
	"message_type": [u'request',],
	"message_format": [u'write',],
	"data": {
		"upsert": dict,
		"delete": list
	}
}

model_request_query = {
	"token": unicode,
	"message_type": [u'request',],
	"message_format": [u'query',],
	"query" : {
		"BB": list,
		"inventory": dict,
		"time": unicode,
		"signed": bool
	},
}


# here the upserts are a list of ids because we list the ids of upsert done rather than put the actual data (transferred in the previous request message)
model_response_write = {
	"token": unicode,
	"message_type": [u'response',],
	"message_format": [u'write',],
	"acknowledge": {
		"upsert": list,
		"delete": list
	},
	"anomalies": list
}

model_error_anomaly = {
	"token": unicode,
	"message_type": [u'error',],
	"message_format": [u'anomaly',],
	"anomalies": list
}

model_error_error = {
	"token": unicode,
	"message_type": [u'error',],
	"message_format": [u'error',],
	"error_message": unicode,
	"error_code": unicode
}

model_request_api = {
	"token": unicode,
	"message_type": [u'request',],
	"message_format": [u'query',],
	"query" : {
		"BB": list,
		"time": unicode,
		"sign": unicode
	},
	"key": unicode,
}

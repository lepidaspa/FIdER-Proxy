#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

template_response_read = {
	"token": u'',
	"message_type": u'response',
	"message_format": u'read',
	"operation": u'',
	"data": {
		"upsert": [],
	}
}

template_request_write = {
	"token": u'',
	"message_type": u'request',
	"message_format": u'write,',
	"data": {
		"upsert": [],
	}
}
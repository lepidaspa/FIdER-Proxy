#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Antonio Vaccarino'
__docformat__ = 'restructuredtext en'

#IMPORTING FROM CONFIG TESTING
#TODO: REPLACE WITH ACTUAL CONFIG DATA
import config_testing



# list of FEDERA user ids that can admin the proxy, can change
users_federa = []

#email address where all errors are sent, can change
#NOTE: this is IN ADDITION to sending to all admins according to FEDERA settings
mail_admin = config_testing.PROXYADMIN_MAIL

# location of the logging file
log_folder = "./tests/logs/"

# general data path for the HARD proxy
baseproxypath = config_testing.HARDPROXY_DATAPATH
baseuploadpath = config_testing.UPLOADPATH


mainserver_ref_location = config_testing.MAINSERVER_CONF_FILE



dir_mirror = 'maps/mirror/'
dir_geojson = 'maps/geojson/'

tries_for_connection = 3



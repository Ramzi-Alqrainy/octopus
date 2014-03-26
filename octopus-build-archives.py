#! /usr/bin/python
# -*- coding: UTF-8 -*-

import time, datetime, getopt, sys, os, os.path

from Octopus.Engine import octopus_templates, octopus_static, LogEngine, EventSettingsEngine

event_settings_engine = EventSettingsEngine()
log_engine = LogEngine(event_settings_engine)

date = datetime.date.today()
print "building archives ..."
log_engine.build_archive(date)

# -*- coding: UTF-8 -*-

import time, getopt, sys, os, os.path

try: import json
except: import simplejson as json

from bottle import *
from .Engine import octopus_templates, octopus_static, LogEngine,EventSettingsEngine

del TEMPLATE_PATH[:]
TEMPLATE_PATH.append(octopus_templates)

event_settings_engine = EventSettingsEngine()
log_engine = LogEngine(event_settings_engine)

BaseRequest.MEMFILE_MAX=1024*1024*100
BaseRequest.MAX_PARAMS=1000

old_route=route
def route(r, *a, **kw):
    return old_route('/ws'+r, *a,**kw)

old_post=post
def post(r, *a, **kw):
    return old_post('/ws'+r, *a,**kw)


@route('/ping')
def ws_ping():
  response.content_type = 'text/javascript; charset=UTF-8'
  return json.dumps({"response":"pong"}, ensure_ascii=False)

@route('/record')
@post('/record')
def ws_record():
  last_row_id = log_engine.log(request.params)# An ordered dictionary that may contain multiple values for each key
  response.content_type = 'text/javascript; charset=UTF-8'
  return json.dumps({'id':last_row_id}, ensure_ascii=False)


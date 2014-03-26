# -*- coding: UTF-8 -*-

import time, getopt, sys, os, os.path, glob

try: import json
except: import simplejson as json

from glob import glob
from bottle import *
from .Engine import octopus_templates, octopus_static, LogEngine,EventSettingsEngine, ArchiveEngine

import datetime

del TEMPLATE_PATH[:]
TEMPLATE_PATH.append(octopus_templates)

event_settings_engine = EventSettingsEngine()
log_engine = LogEngine(event_settings_engine)
archive_engine = ArchiveEngine(event_settings_engine)


@route('/static/:fn#.*#')
def web_serve_static(fn):
  response.set_header('Cache-Control', 'max-age=3600, public')
  return static_file(fn, root=octopus_static)

@route('/favicon.ico')
def web_favico():
  response.set_header('Cache-Control', 'max-age=3600, public')
  return static_file('img/favicon.ico', root=octopus_static)

# NOTE: if you want all ui-related urls to be prefixed with ui uncomment the following lines
old_route=route
def route(r, *a, **kw):
    return old_route('/ui'+r, *a,**kw)

old_post=post
def post(r, *a, **kw):
    return old_post('/ui'+r, *a,**kw)

@route('/')
def ui_root():
  redirect('/ui/live');

@route('/live')
@view('live.html')
def ui_live():
  known_events=set(map(lambda p: os.path.basename(p)[6:-5], glob(os.path.join(octopus_templates,'event_*.html'))))
  events=list(log_engine.query('SELECT * FROM log ORDER BY time DESC LIMIT 10'))
  last_id=events[0]['id'] if events else -1
  event_divs=[]
  js_files=['/static/js/jquery.min.js', '/static/js/main.js']
  css_files=['/static/css/main.css']
  for event in events:
      tpl="event_"+event['event']+".html" if event['event'] in known_events else "event_generic.html"
      event_divs.append(''.join(template(tpl, **event)))
  return locals()

@route('/ajax/live')
def ui_live():
  last_id=request.params.get('last_id',-1)
  known_events=set(map(lambda p: os.path.basename(p)[6:-5], glob(os.path.join(octopus_templates,'event_*.html'))))
  events=list(log_engine.query('SELECT * FROM log WHERE id>? ORDER BY time DESC LIMIT 10', (last_id,)))
  event_divs=[]
  for event in events:
      tpl="event_"+event['event']+".html" if event['event'] in known_events else "event_generic.html"
      event_divs.append(''.join(template(tpl, **event)))
  content=''.join(template('live_events_block', event_divs=event_divs))
  if events: last_id=events[0]['id']
  response.content_type = 'text/javascript; charset=UTF-8'
  return json.dumps({"last_id":last_id, 'count':len(events), 'content':content}, ensure_ascii=False)

#@route('/row_query')
#@view('row_query.html')
#def ui_row_query():
  #js_files=['/static/js/jquery.min.js', '/static/js/jquery.datepick.pack.js','/static/js/main.js']
  #css_files=['/static/css/main.css', '/static/css/redmond.datepick.css']
  #date_range=request.params.get('date_range','')
  #query=request.params.get('query','SELECT event, count(*) FROM log GROUP BY event')
  #results=[]
  #return locals()

#@post('/row_query')
#@view('row_query.html')
#def ui_row_query_post():
  #js_files=['/static/js/jquery.min.js', '/static/js/jquery.datepick.pack.js','/static/js/main.js']
  #css_files=['/static/css/main.css', '/static/css/redmond.datepick.css']
  #date_range=request.params.get('date_range','')
  #query=request.params.get('query','SELECT event, count(*) FROM log GROUP BY event')
  #d1,d2=date_range.split(":",1)
  #d1=datetime.datetime.strptime(d1.strip(), '%Y-%m-%d').date()
  #d2=datetime.datetime.strptime(d2.strip(), '%Y-%m-%d').date()
  #results=archive_engine.map_query(archive_engine.partitions_by_date(d1,d2), query)
  #return locals()

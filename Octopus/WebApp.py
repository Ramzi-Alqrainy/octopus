# -*- coding: UTF-8 -*-

from .WebService import *
from .WebUI import *

from bottle import *

__all__=['application', 'cli']

@route('/')
def app_root():
    redirect('/ui/');


application = app()

def usage(n=0):
  e=os.path.basename(sys.argv[0])
  print """%s - a web service for Octopus
Usage: %s -h -d -r -H <HOST> -p <PORT> -s <SERVER>
Options are:
  * -h		- this help message
  * -d		- debug mode (show backtrace)
  * -r		- auto-reload on any change
  * -H HOST	- where HOST can be localhost (default) or 0.0.0.0 for any
  * -p PORT	- PORT defaults to 8080
  * -s SERVER	- where SERVER can be:
  			* bjoern (recommended)
  			* wsgiref (default)
  			* paste (for python-paste)
  			* flup (for factcgi)
""" % (e,e)
  sys.exit(n)

def cli():
  try:
    optlist, args = getopt.getopt(sys.argv[1:], 'drhH:p:s:', ['help'])
  except getopt.GetoptError, err:
    print str(err)
    usage(2)
  opt=dict(optlist)
  if opt.has_key('-h') or opt.has_key('--help'): usage()
  if opt.has_key('-d'): debug(True)
  else: debug(False)
  run(app=application, reloader=opt.has_key('-r'), host=opt.get('-H', 'localhost'), port=opt.get('-p', '8080'), server=opt.get('-s', 'wsgiref'))

if __name__ == '__main__':
  cli()


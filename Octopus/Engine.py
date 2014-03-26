# -*- coding: UTF-8 -*-

import sys, os, os.path, time, datetime , threading, sqlite3
import fcntl

try: import json
except ImportError: import simplejson as json

from itertools import *
from glob import iglob

import bson

from .SqliteBackend import Backend,PartitionedBackend
from OctopusSettings import *
from .FileBasedLock import *

class EventSettingsEngine(Backend):
    schema={'event_settings':
        '''CREATE TABLE event_settings (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  url_event VARCHAR(128) NULL,
  db_event VARCHAR(128) NULL,
  type_of_event VARCHAR(128) NULL
  )'''}
  
    def __init__(self,*a, **kw):
      Backend.__init__(self, *a, **kw)
      self.get_all_params()

    def _init_db(self, cn):
        Backend._init_db(self, cn)
        self.execute('BEGIN')
        self.execute('INSERT  INTO  event_settings (type_of_event, url_event,db_event) Values ("search","query","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","query","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","rank","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","place","b")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search","ALG_type","b")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search","no_of_results","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search","spell_query","c")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","place_id","j")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search","version_of_algorithm","k")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search","version_of_search","l")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","version_of_algorithm","k")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_click","version_of_search","l")')
        self.execute('INSERT  INTO  event_settings (type_of_event, url_event,db_event) Values ("search_filter","query","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_filter","no_of_results","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_filter","version_of_algorithm","k")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_filter","version_of_search","l")')
        self.execute('INSERT  INTO  event_settings (type_of_event, url_event,db_event) Values ("search_pagination","query","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_pagination","page","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_pagination","version_of_algorithm","k")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("search_pagination","version_of_search","l")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("email","email_type","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("email","destination","b")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("add_review","review_id","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("add_review","version","j")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("page_view","route","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("page_view","title","b")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("page_view","millisecond","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("page_view","code","j")')
        
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("error","route","a")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("error","severity","b")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("error","message","c")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("error","millisecond","i")')
        self.execute('INSERT INTO  event_settings (type_of_event, url_event,db_event) Values ("error","code","j")')

        self.execute('COMMIT')
      
    def get_all_params(self):
        p={}
        q=self.query('select type_of_event, url_event,db_event from event_settings order by type_of_event')
        for event_type,rows in groupby(q, lambda d: d['type_of_event']):
           p[event_type]=dict([(d['url_event'], d['db_event']) for d in rows])
        self._params=p
        return p

    def get_params(self, event):
        if self._params == None: self.get_all_params()
        return self._params.get(event, {})

    def get_filename(self):
        return os.path.join(octopus_data, 'event_settings.db')

class LogEngine(Backend):
    schema={'log':
        '''CREATE TABLE log (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  event VARCHAR(20) NOT NULL,
  time INTEGER NOT NULL,
  time_s VARCHAR(20) NOT NULL, -- 2012-02-16 12:22:56 -- can be used as a prefix
  server_host VARCHAR(32) NOT NULL DEFAULT '',
  user_id VARCHAR(32) NOT NULL DEFAULT 'guest',
  session VARCHAR(32) NOT NULL DEFAULT '',
  user_gender INTEGER NULL, -- NULL unknown, 1 male, 2 female
  user_age INTEGER NULL, -- in years, NULL unknown
  user_agent VARCHAR(128) NOT NULL DEFAULT '',
  browser_family VARCHAR(128) NULL,
  ip VARCHAR(128) NOT NULL DEFAULT '',
  ip_country VARCHAR(128) NULL,
  city VARCHAR(128) NOT NULL DEFAULT '',
  country_code VARCHAR(128) NOT NULL DEFAULT '',
  screen_width INTEGER NULL,
  screen_height INTEGER NULL,
  url VARCHAR(128) NOT NULL DEFAULT '',
  uri VARCHAR(128) NOT NULL DEFAULT '',
  domain VARCHAR(128) NOT NULL DEFAULT '',
  http_ref VARCHAR(128) NOT NULL DEFAULT '',
  http_ref_domain VARCHAR(128) NOT NULL DEFAULT '',
  http_ref_uri VARCHAR(128) NOT NULL DEFAULT '',
  utmz VARCHAR(128) NOT NULL DEFAULT '', -- raw utmz cookie value
  utm_source VARCHAR(128) NOT NULL DEFAULT '',
  utm_campaign VARCHAR(128) NOT NULL DEFAULT '',
  utm_medium VARCHAR(128) NOT NULL DEFAULT '',
  utm_term VARCHAR(128) NOT NULL DEFAULT '',
  utm_content VARCHAR(128) NOT NULL DEFAULT '',
  a VARCHAR(128) NULL,
  b VARCHAR(128) NULL,
  c VARCHAR(128) NULL,
  d VARCHAR(128) NULL,
  i INTEGER NULL, -- nullable, because we want to have NULL and 0
  j INTEGER NULL,
  k INTEGER NULL, -- nullable, because we want to have NULL and 0
  l INTEGER NULL,
  data BLOB NOT NULL DEFAULT ''
  )''',
    }
    index={'log': ('time',)
    }
    int_params=set(('time', 'user_gender', 'user_age', 'screen_width', 'screen_height', 'i', 'j', 'k', 'l',))
    _event_counter_limit=100 # the limit on which to check if an archive process is needed
    def __init__(self, event_settings, *a, **kw):
      Backend.__init__(self, *a, **kw)
      self._event_counter=0 # a counter used internally to impose some limit on archiving
      self.encode=bson.BSON.encode
      self.decode=bson.BSON.decode
      self.event_settings=event_settings
      self.oldest_date = self.get_oldest_record_date()

    def get_oldest_record_date(self): 
      query_time = list(self.query('select time from log order by time limit 1'))
      if not query_time: return None
      return datetime.date.fromtimestamp(query_time[0]['time'])

    def get_filename(self):
        return os.path.join(octopus_data, 'log.db')

    def _int_or_unicode(self, kv):
        key, value=kv
        if key in self.int_params:
            try: return (key, int(value))
            except ValueError: return (key, None)
        return (key, value.decode('UTF-8', 'replace'))

    def build_archive(self, date):
        with FileBasedLock(os.path.join(octopus_data, 'archives.lock')):
            max_id=0
            archive = ArchiveEngine(self.event_settings)
            # Convert Date to timestamp
            timestamp = int(time.mktime(date.timetuple()))
            q = self.query('SELECT * FROM log WHERE time < ? ORDER BY time', (timestamp, ))
            # get rows for each partition key
            for key, rows in groupby(q, lambda r: r['time_s'][:10]):
                archive.execute(key, 'BEGIN IMMEDIATE')
                self.execute('BEGIN IMMEDIATE')
                ids = []
                for row in rows:
                    # Transfer event rows from log to Archive
                    ids.append(str(row['id']))
                    if row['id']>max_id: max_id=row['id']
                    del row['id']
                    archive.insert(key, 'log',row)
                archive.execute(key, 'COMMIT')
                #self.execute('DELETE FROM log WHERE id in  < ? AND id <= ?', (timestamp, max_id,) ) # we don't have index on both
                if (ids): self.execute('DELETE FROM log WHERE id IN (%s)' % ( ', '.join(ids), ))
                self.execute('COMMIT')
                del ids[:]
                max_id=0
                time.sleep(1) # wait 1 second
            self.oldest_date = self.get_oldest_record_date()
        
    def log(self, request_params): 
      with FileBasedReadLock(os.path.join(octopus_data, 'archives.lock')):
        event = request_params['event'] # search , search_click , ...
        param_mapping = self.event_settings.get_params(event) # eg. {'query':'a', 'rank':'i'}
        # Replace the key of request url to column of DB to insert it later and
        # build dictionary
        params = dict(map(self._int_or_unicode, map(lambda (key,value): (param_mapping.get(key, key), value), request_params.items())))
        # Get keys from unmapped params
        keys_of_params = params.keys()
        # Get Columns name
        col_name_list = self.get_cols()['log']
        # get all keys are not in the columns name list to store it in data 
        diff = set(keys_of_params).difference(col_name_list)
        data = dict(map(lambda(key): (key,params.pop(key)),diff))
        params['data'] = sqlite3.Binary(self.encode(data))
      
        try: params['time']=int(params['time'])
        except ( ValueError , TypeError , KeyError ): params['time']=int(time.time())
        date = datetime.date.fromtimestamp(params['time'])
        params['time_s'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(params['time']))
        # insert into logs
        row_count,last_row_id=self.insert('log',params)
        self._event_counter+=1
        # The System builds Archiving file , when the date of event is grater than
        # the oldest date in log DB 
        if self.oldest_date:
          if date > self.oldest_date and self._event_counter>=self._event_counter_limit:
              # reset counter and start arching in another thread
              self._event_counter=0
              threading.Thread(target=self.build_archive, args=(date,)).start()
        else: self.oldest_date = date
        return last_row_id
       
class ArchiveEngine(PartitionedBackend):
    schema={'log':
        '''CREATE TABLE log (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  event VARCHAR(20) NOT NULL,
  time INTEGER NOT NULL,
  time_s VARCHAR(20) NOT NULL, -- 2012-02-16 12:22:56 -- can be used as a prefix
  server_host VARCHAR(32) NOT NULL DEFAULT '',
  user_id VARCHAR(32) NOT NULL DEFAULT 'guest',
  session VARCHAR(32) NOT NULL DEFAULT '',
  user_gender INTEGER NULL, -- NULL unknown, 1 male, 2 female
  user_age INTEGER NULL, -- in years, NULL unknown
  user_agent VARCHAR(128) NOT NULL DEFAULT '',
  browser_family VARCHAR(128) NULL,
  ip VARCHAR(128) NOT NULL DEFAULT '',
  ip_country VARCHAR(128) NULL,
  city VARCHAR(128) NOT NULL DEFAULT '',
  country_code VARCHAR(128) NOT NULL DEFAULT '',
  screen_width INTEGER NULL,
  screen_height INTEGER NULL,
  url VARCHAR(128) NOT NULL DEFAULT '',
  uri VARCHAR(128) NOT NULL DEFAULT '',
  domain VARCHAR(128) NOT NULL DEFAULT '',
  http_ref VARCHAR(128) NOT NULL DEFAULT '',
  http_ref_domain VARCHAR(128) NOT NULL DEFAULT '',
  http_ref_uri VARCHAR(128) NOT NULL DEFAULT '',
  utmz VARCHAR(128) NOT NULL DEFAULT '', -- raw utmz cookie value
  utm_source VARCHAR(128) NOT NULL DEFAULT '',
  utm_campaign VARCHAR(128) NOT NULL DEFAULT '',
  utm_medium VARCHAR(128) NOT NULL DEFAULT '',
  utm_term VARCHAR(128) NOT NULL DEFAULT '',
  utm_content VARCHAR(128) NOT NULL DEFAULT '',
  a VARCHAR(128) NULL,
  b VARCHAR(128) NULL,
  c VARCHAR(128) NULL,
  d VARCHAR(128) NULL,
  i INTEGER NULL, -- nullable, because we want to have NULL and 0
  j INTEGER NULL,
  k INTEGER NULL, -- nullable, because we want to have NULL and 0
  l INTEGER NULL,
  data BLOB NOT NULL DEFAULT ''
  )''',
    }
    index={'log': ('url','uri','server_host','domain','event', 'time_s', 'time','user_id', 'session', 'user_gender','user_age', 'ip', 'ip_country', 'city', 'country_code','browser_family','screen_width', 'screen_height', 'http_ref','http_ref_domain','http_ref_uri','utmz','utm_source','utm_campaign','utm_medium','utm_term','utm_content', 'a', 'b','c', 'd','i','j','k','l',)
    }
    _oldest_date = None
    def __init__(self, event_settings, *a, **kw):
        PartitionedBackend.__init__(self, *a, **kw)
        self.event_settings=event_settings
      
    def get_partition_filename(self, key):
        if len(key)!=10: raise ValueError
        yyyy, yyyymm = key[:4], key[:7]
        d=os.path.join(octopus_data, 'archives', yyyy, yyyymm)
        return os.path.join(d, key+'.db')

    def get_oldest_year(self):
        """return minimum year, this method may raise ValueError"""
        return os.path.basename(min(iglob(os.path.join(octopus_data, 'archives', '[0-9][0-9][0-9][0-9]'))))

    def get_oldest_year_month(self, y):
        """return minimum month, this method may raise ValueError"""
        return os.path.basename(min(iglob(os.path.join(octopus_data, 'archives', y, y+'-[0-9][0-9]'))))

    def get_oldest_date(self):
        if self._oldest_date!=None: return self._oldest_date
        try: 
            y=self.get_oldest_year()
            ym=self.get_oldest_year_month(y)
            key=os.path.basename(min(iglob(os.path.join(octopus_data, 'archives', y, ym, ym+'-[0-9][0-9].db'))))[:-3]
        except ValueError: return datetime.date.today()
        y,m,d=key.split('-')
        self._oldest_date=datetime.date(y,m,d)
        return self._oldest_date

    def get_oldest_record_timestamp(self):
        return time.mktime(self.get_oldest_date().timetuple())

    def partition_by_timestamp(self, t1=None):
        if t1==None: t1=time.time()
        s=time.localtime(t1) # localtime is used instead of gmtime() because datetime is local
        return "%04d-%02d-%02d" % (s.tm_year, s.tm_mon, s.tm_mday)

    def partition_by_date(self, d=None):
        if d==None: d=datetime.date.today()
        return "%04d-%02d-%02d" % (d.year, d.month, d.day)

    def partitions_by_date(self, d1=None, d2=None):
        if d1==None: d1=self.get_oldest_date()
        if d2==None: d2=datetime.date.today()
        d=d1
        day=datetime.timedelta(1)
        while(d<=d2):
            key=self.partition_by_date(d)
            if os.path.exists(self.get_partition_filename(key)): yield key
            d+=day

    def partitions_by_timestamp(self, t1=None, t2=None):
        if t1==None: self.get_oldest_record_timestamp() 
        if t2==None: t2=time.time()
        d1=datetime.date.fromtimestamp(t1)
        d2=datetime.date.fromtimestamp(t2)
        return self.partitions_by_date(d1, d2)

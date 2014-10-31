import os
from datetime import datetime
from collections import defaultdict
from subprocess import Popen, PIPE

sout,serr = Popen(["which", "cql2"], stdout=PIPE, stderr=PIPE).communicate()
if len(serr) > 0:
  raise Exception(serr)
if len(sout) == 0:
  raise Exception("Could not find 'cql2' in path. Library requires this binary.")


def _parse_dt(x):
  if isinstance(x, list):
    x = ' '.join(x)
  return datetime.strptime(x, "%Y-%m-%d %H:%M:%S+00:00")


def query(zone, op=None, service=None, host=None, metric=None, args=None, query=None, latest_only=False):
  """ Usage:

        op      = sum/avg/min/max
        service = kestrel
        host    = <hostname>/colony/members
        metric  = <metric name>

        OR 

        query = <cql query>

      Return:
        
        If multiple data sets then data is returned as:
          {<dataset>: [(datetime, value)]}

        If single data set:
          [(datetime, value)]
  """
  cmd = ['cql2','-z',zone,'q']
  if latest_only:
    args = ['-d', '300'] # 5 min history only
  elif args is None:
    args = []
  cmd.extend(args)

  if query:
    cmd.append(query)
  else:
    cmd.extend(['-o', op, service, host, metric])
  sout, serr = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
  if len(serr) > 0:
    raise Exception(serr)

  nest_data = defaultdict(list)
  flat_data = []
  for line in (l.strip() for l in sout.split('\n') if l.strip() != ''):
    try:
      sdata = line.split()

      if len(sdata) == 4: # nested data
        # <dataset>: <date> <time> <value>
        key = sdata[0].strip(':')
        dt = _parse_dt(sdata[1:3])
        value = float(sdata[3])
        if latest_only:
          nest_data[key] = [(dt, value)]
        else:
          nest_data[key].append((dt, value))

      elif len(sdata) == 3: # single data set
        # <date> <time> <value>
        dt = _parse_dt(sdata[0:2])
        value = float(sdata[2])
        
        if host is not None: # nest for single host data
          if latest_only:
            nest_data[host] = [(dt, value)]
          else:
            nest_data[host].append((dt, value))

        else: # otherwise flat
          flat_data.append((dt, value))

      else: # invalid format
        raise Exception("Unexpected data format: {}", sdata)

    except Exception, e:
      raise Exception("Failed parsing record {}\n{}", sdata, e)

  if len(nest_data) > 0:
    return nest_data
  else:
    return flat_data

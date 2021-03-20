"""
Small program to download last N days from Nightscout
and put them into the format the tuner expects.
"""
from datetime import datetime, timedelta, date
import argparse
import dateutil.parser
import hashlib
import collections
import json
import pytz
import sys
import time
import requests
import pytz
from pprint import pprint


TZ='Europe/Berlin' 


class Nightscout(object):

  def __init__(self, url, secret=None):
    self.url = url
    self.secret = None
    if secret:
        self.secret = hashlib.sha1(secret.encode('utf-8')).hexdigest()
    self.batch = []


  def download(self, path, params=None):
    url = self.url + '/api/v1/' + path + '.json'
    headers = {
            'Content-Type': 'application/json',
    }
    if self.secret:
        headers.update({
            'api-secret': self.secret,
        })
    response = requests.get(url, params=params, headers=headers)
    if response.status_code != 200:
        print(response.status_code, response.text)
        raise Exception(response.status_code, response.text)
    return response.json()

  def convert(self, profile, entries, treatments):
    ps = profile[0]['store']['Default']
    tz = pytz.timezone(ps['timezone'])
    ret = {
            'version': 1,
            'timezone': ps['timezone'],
            'minimum_time_interval': 3600,
            'maximum_schedule_item_count': 24,
        #    'allowed_basal_rates': [n/10.0 for n in range(1, 20)],
        #    'tuning_limit': 0.4,
            'basal_insulin_parameters': {
                # +- Fiasp, not sure about duration
                'delay': 8,
                'peak': 44,
                'duration': 200,
            },
            'insulin_sensitivity_schedule': {
                'index': [int(x['timeAsSeconds'] / 60) for x in ps['sens']],
                'values': [x['value'] for x in ps['sens']],
            },
            'carb_ratio_schedule': {
                'index': [int(x['timeAsSeconds'] / 60) for x in ps['carbratio']],
                'values': [x['value'] for x in ps['carbratio']],
            },
            'basal_rate_schedule': {
                'index': [int(x['timeAsSeconds'] / 60) for x in ps['basal']],
                'values': [x['value'] for x in ps['basal']],
            },
            'timelines': [],
    }
    def lookup_basal(hour):
        minutes = hour * 60
        for i, v in enumerate(ret['basal_rate_schedule']['index']):
            if v >= minutes:
                return ret['basal_rate_schedule']['values'][i]
        return ret['basal_rate_schedule']['values'][-1]

    glucose = {
            'type': 'glucose',
            'index': [],
            'values': [],
    }
    min_dt = None
    max_dt = None
    offset = None
    offset_sgv = None
    for e in sorted(entries, key=lambda x: x['dateString']):
        dt = dateutil.parser.parse(e['dateString'])
        # print(e['dateString'], dt)
        min_dt = min(min_dt or dt, dt)
        max_dt = min(max_dt or dt, dt)
        ots = int(datetime.timestamp(dt))
        if offset is None:
            ts = ots
            sgv = e['sgv']
        else:
            ts = ots - offset
            sgv = e['sgv'] - offset_sgv
        glucose['index'].append(ts)
        glucose['values'].append(sgv)
        offset = ots
        offset_sgv = e['sgv']
    print('MIN', min_dt, 'MAX', max_dt)
    ret['timelines'].append(glucose)

    basal = []
    bolus = []
    carbs = collections.defaultdict(list)
    for t in sorted(treatments, key=lambda x: x['created_at']):
        dt = dateutil.parser.parse(t['created_at'])
        lt = dt.astimezone(tz)
        ts = int(datetime.timestamp(dt))
        if t['eventType'] == 'Temp Basal':
           # if the temp basal is longer than the schedule,
           # needs to split up in 30 minute intervals.
           default_basal = lookup_basal(lt.hour)
           delta = t['rate'] - default_basal
           # print(dt, lt, t['rate'], default_basal, delta)
           basal.append((ts, delta, t['duration']*60))
        elif t['eventType'] == 'Correction Bolus':
           bolus.append((ts, t['insulin']))
        elif t['eventType'] == 'Meal Bolus':
           carbs[t['absorptionTime']].append((ts, t['carbs']))
        elif t['eventType'].startswith('Debug.'):
            pass
        else:
           print('ignored', t['eventType'])
    
    basal_timeline = {
            'type': 'basal',
            'parameters': ret['basal_insulin_parameters'],
            'index': [],
            'values': [],
            'durations': [],
            'ots': [],
    }
    offset = None
    active_until = None
    for ts, rate, duration in basal:
        ots = ts
        if offset is None:
            if duration == 0:
                continue
            offset = ts
        else:
            ts = ots - offset

        if duration == 0:
            delta = ots - basal_timeline['ots'][-1]
            basal_timeline['durations'][-1] = delta
            active_until = None
            continue

        if active_until and active_until > ots:
            pts = basal_timeline['ots'][-1]
            delta = ots - pts

            print(ots, ts, rate, duration)
            if rate == basal_timeline['values'][-1]:
                basal_timeline['durations'][-1] += delta
                active_until += delta 
                print('  adjust basal', pts, delta)
                continue
            else:
                basal_timeline['durations'][-1] = delta
                print('  cancel basal', pts, delta)

        if len(basal_timeline['index']) and ts == basal_timeline['index'][-1]:
            print('tweak duplicate basal ts', ts)
            ts += 1
        basal_timeline['index'].append(ts)
        basal_timeline['values'].append(rate)
        basal_timeline['durations'].append(duration)
        basal_timeline['ots'].append(ots)
        active_until = ots + duration
        offset = ots

    del basal_timeline['ots']
    ret['timelines'].append(basal_timeline)

    bolus_timeline = {
            'type': 'bolus',
            'parameters': ret['basal_insulin_parameters'],
            'index': [],
            'values': [],
    }
    offset = None
    for ts, units in bolus:
        ots = ts
        if offset is None:
            offset = ts
        else:
            ts = ots - offset
        if len(bolus_timeline['index']) and ts == bolus_timeline['index'][-1]:
            print('tweak duplicate bolus ts', ots, ts)
            ts += 1
        bolus_timeline['index'].append(ts)
        bolus_timeline['values'].append(units)
        offset = ots

    ret['timelines'].append(bolus_timeline)

    for absorption, items in carbs.items():
        carb_timeline = {
            'type': 'carb',
            'parameters': { 'delay': 5.0, 'duration': absorption },
            'index': [],
            'values': [],
        }
        offset = None
        for ts, amount in items:
            ots = ts
            if offset is None:
                offset = ts
            else:
                ts = ots - offset
            if len(carb_timeline['index']) and ts == carb_timeline['index'][-1]:
                print('tweak duplicate carb ts', ts)
                ts += 1
            carb_timeline['index'].append(ts)
            carb_timeline['values'].append(amount)
            offset = ots
        ret['timelines'].append(carb_timeline)

    # pprint(ret)
    return ret


if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--url", type=str, help="nightscout url")
  parser.add_argument("--secret", type=str, help="nightscout secret")
  parser.add_argument("--days", type=int, help="days to retrieve since yesterday")
  args = parser.parse_args()

  days = int(args.days or 1)

  today = datetime.combine(date.today(), datetime.min.time())
  dl = Nightscout(args.url, args.secret)
  
  cache_fn = 'cache_%s_%d.json' % (today.isoformat(), days)
  j = {}
  try:
    j = json.loads(open(cache_fn).read())
  except IOError:
    pass

  profile = j.get('p') or dl.download('profile')
  tz = profile[0]['store']['Default']['timezone']
  #today = today.replace(tzinfo=pytz.timezone(tz))
  today = today.astimezone(pytz.timezone(tz))
  startdate = today - timedelta(days=days + 1)
  enddate = startdate + timedelta(days=days) 
  startdate = startdate.astimezone(pytz.utc).isoformat()
  enddate = enddate.astimezone(pytz.utc).isoformat()
  print(startdate, enddate)

  treatments = j.get('t') or dl.download(
          'treatments',
          {'find[created_at][$gte]': startdate, 'find[created_at][$lte]': enddate})
  entries = j.get('e') or dl.download(
          'entries',
          {'find[dateString][$gte]': startdate, 'find[dateString][$lte]:': enddate, 'count': '10000'})
  if not j:
    open(cache_fn, 'w').write(json.dumps({'p': profile, 'e': entries, 't': treatments}, indent=4, sort_keys=True))
  ret = dl.convert(profile, entries, treatments)
  output_fn = 'ret_%s_%s.json' % (startdate, enddate)
  open(output_fn, 'w').write(json.dumps(ret, indent=4, sort_keys=True))
  print('Written', output_fn)

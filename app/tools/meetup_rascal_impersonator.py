import meetup.api as mapi
import numpy as np
import random
from os import environ
from time import sleep
import sys

API_KEY = environ['MEETUP_API_KEY']

client = mapi.Client(API_KEY)

rsvp_dict = {'rsvp': 'yes', 'agree_to_refund': 'false', 'opt_to_pay': 'false', 'event_id': None}

events_warsaw = list(map(lambda event: event.get('id', None), client.GetOpenEvents({'country': 'pl', 'city': 'Warsaw'}).results))
events_london = list(map(lambda event: event.get('id', None), client.GetOpenEvents({'country': 'gb', 'city': 'London'}).results))

events = events_warsaw + events_london
good_events = list()

# get random events
events_threshold = 5

# while True:
#     for event_id in events:
#         rsvp_dict.update({'event_id': event_id})
#
#         try:
#             rsvp = client.CreateRsvp(rsvp_dict)
#         except:
#             pass
#
#         try:
#             _ = rsvp.problem
#         except AttributeError:
#             good_events.append(event_id)
#
#         if len(good_events) > events_threshold:
#             print(good_events)
#             for event_id in good_events:
#                 rsvp_dict.update({'event_id': event_id})
#
#                 try:
#                     rsvp = client.CreateRsvp(rsvp_dict)
#                     print('done')
#                 except:
#                     pass
#
#             sys.exit(0)


good_events = ['256571132', '256563740', '256464875', '256143578', '256141115', '256712495']

for event_id in good_events:
    rsvp_dict.update({'event_id': event_id})

    try:
        rsvp = client.CreateRsvp(rsvp_dict)
        sleep(1)
        print('done')
    except:
        pass

sys.exit(0)
import datetime
# import pytz
# from tzwhere import tzwhere

from app.gcp_pubsub.subscribers.base import BaseSubscriber


class MeetupRsvpSubscriber(BaseSubscriber):
    def __init__(self, *args, **kwargs):
        super(MeetupRsvpSubscriber, self).__init__(*args, **kwargs)

        # self.tzwhere = tzwhere.tzwhere()

    def enrich(self, message):
        group_topics = message.get("group", dict()).get("group_topics", dict())

        if group_topics:
            message['group']['group_topics'] = [d['topic_name'] for d in message['group']['group_topics']]

        # geo handling
        group_geo_lat = message.get("group", dict()).get("group_lat", None)
        group_geo_lon = message.get("group", dict()).get("group_lon", None)

        if group_geo_lon and group_geo_lat:
            message['group']['group_geo'] = MeetupRsvpSubscriber.create_geo_object(group_geo_lat, group_geo_lon)

        venue_geo_lat = message.get("venue", dict()).get("lat", None)
        venue_geo_lon = message.get("venue", dict()).get("lon", None)

        if venue_geo_lon and venue_geo_lat:
            message['venue']['venue_geo'] = MeetupRsvpSubscriber.create_geo_object(venue_geo_lat, venue_geo_lon)

            # event_time = message.get("event", dict()).get("event_time", None)
            #
            # if event_time:
            #     message['event'].update(self.utc_epoch_to_local_by_coords(event_time,
            #                                                                        venue_geo_lat,
            #                                                                        venue_geo_lon))

        return message

    # def utc_epoch_to_local_by_coords(self, epoch_utc, lat, lon):
    #
    #     # check if provided in ms or s:
    #     if len(str(epoch_utc)) == 13:
    #         epoch_utc = epoch_utc / 1000
    #
    #     timezone_str = self.tzwhere.tzNameAt(lat, lon)  # lat lon
    #
    #     # get time in UTC
    #     utc_dt = datetime.utcfromtimestamp(epoch_utc)
    #
    #     # convert it to tz
    #     tz = pytz.timezone(timezone_str)
    #     dt = utc_dt.astimezone(tz)
    #
    #     offset = dt.utcoffset().total_seconds()
    #
    #     local_dt = datetime.utcfromtimestamp(epoch_utc + offset)
    #
    #     return dict(month_local=local_dt.month, day_local=local_dt.day, weekday_local=local_dt.weekday(),
    #                 hour_local=local_dt.hour, minute_local=local_dt.minute)
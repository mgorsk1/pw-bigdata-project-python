from app.gcp_pubsub.subscribers.base import BaseSubscriber


class MeetupRsvpSubscriber(BaseSubscriber):
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

        return message

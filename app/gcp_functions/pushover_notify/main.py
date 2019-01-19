def pushover_notify(data, context):
    from requests import post, HTTPError
    from json import loads, JSONDecodeError
    from base64 import b64decode
    from os import getenv

    if isinstance(data, dict):
        notification = data
    else:
        try:
            notification = loads(data)
        except Exception as e:
            print(e.args, data)

    config = dict(APP_TOKEN=getenv("PUSHOVER_APP_TOKEN"), USER_KEY=getenv("PUSHOVER_USER_KEY"))

    def notify(title, message, url):
        """
        Send request to pushover_notify API with data:
    
        token (required) - your application"s API token
        user (required) - the user/group key (not e-mail address) of your user (or you), viewable when logged into our dashboard (often referred to as USER_KEY in our documentation and code examples)
        message (required) - your message
        Some optional parameters may be included:
        attachment - an image attachment to send with the message; see attachments for more information on how to upload files
        device - your user"s device name to send the message directly to that device, rather than all of the user"s devices (multiple devices may be separated by a comma)
        title - your message"s title, otherwise your app"s name is used
        url - a supplementary URL to show with your message
        url_title - a title for your supplementary URL, otherwise just the URL is shown
        priority - send as -2 to generate no notification/alert, -1 to always send as a quiet notification, 1 to display as high-priority and bypass the user"s quiet hours, or 2 to also require confirmation from the user
        sound - the name of one of the sounds supported by device clients to override the user"s default sound choice
        timestamp - a Unix timestamp of your message"s date and time to display to the user, rather than the time your message is received by our API
    
        :param title: title for message
        :param message: message
        :param url: optional url to pass
        :return: -
        """

        request_data = dict(token=config.get("APP_TOKEN", ""),
                            user=config.get("USER_KEY", ""),
                            message=message,
                            device="pw-bd-project-funct-pushover_notify",
                            title=title,
                            url=url)

        try:
            result = post("https://api.pushover.net/1/messages.json", data=request_data)
        except:
            raise HTTPError

        print(result)

    if "data" in notification:
        notification_data_decoded = b64decode(notification["data"]).decode("utf-8")

        if isinstance(notification_data_decoded, dict):
            notification_data = notification_data_decoded
        else:
            try:
                notification_data = loads(notification_data_decoded)
            except Exception as e:
                raise JSONDecodeError

        notify(notification_data.get("title", ""),
               notification_data.get("message", "na"),
               notification_data.get("url", ""))



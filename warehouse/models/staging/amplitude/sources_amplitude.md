{% docs amplitude_user_id %}
    The user ID of the user. This should be a unique user identifier of the user
    and should never change for the same user, e.g. a hashed string of the user's username.

    This is explicitly set by the customer; Amplitude does not auto-populate this field.
{% enddocs %}

{% docs amplitude_event_time %}
    Amplitude timestamp (UTC) which is the client_event_time adjusted by the difference
    between server_received_time and client_upload_time.

    Learn more at https://help.amplitude.com/hc/en-us/articles/229313067#Raw-Data-Fields
    or https://www.docs.developers.amplitude.com/data/destinations/snowflake/#event-table-schema
{% enddocs %}

{% docs amplitude_user_properties %}
    User properties are the attributes of individual users. Common user properties include
    device type, location, User ID, and whether the user is a paying customer or not. An
    attribute can reflect either current or previous values, depending on its nature and
    how often it is updated.

    Amplitude sends user properties with every event.
    Learn more at https://help.amplitude.com/hc/en-us/articles/115002380567
{% enddocs %}

def extract_user_attributes(session_data):
    attributes = []
    for attribute, value in session_data.get("userAttributes", {}).items():
        attributes.append({
            "sessionId": int(session_data["sessionId"]),
            "name": attribute,
            "value": value,
        })
    return attributes


def extract_states(session_data):
    """
    We don't seem to use states; csv export returns empty states csv's
    stateId in the exported json is a random number assigned to an event according to
    https://docs.leanplum.com/docs/reading-and-understanding-exported-sessions-data
    """
    return []


def extract_experiments(session_data):
    experiments = []
    for experiment in session_data.get("experiments", []):
        experiments.append({
            "sessionId": int(session_data["sessionId"]),
            "experimentId": experiment["id"],
            "variantId": experiment["variantId"],
        })
    return experiments


def extract_events(session_data):
    events = []
    event_parameters = []
    for state in session_data.get("states", []):
        for event in state.get("events", []):
            events.append({
                "sessionId": int(session_data["sessionId"]),
                "stateId": state["stateId"],
                "eventId": event["eventId"],
                "eventName": event["name"],
                "start": event["time"],
                "value": event["value"],
                "info": event.get("info"),
                "timeUntilFirstForUser": event.get("timeUntilFirstForUser"),
            })
            for parameter, value in event.get("parameters", {}).items():
                event_parameters.append({
                    "eventId": event["eventId"],
                    "name": parameter,
                    "value": value,
                })

    return events, event_parameters


def extract_session(session_data, session_columns):
    # mapping from name in destination table to name in source data
    field_name_mappings = {
        "timezoneOffset": "timezoneOffsetSeconds",
        "osName": "systemName",
        "osVersion": "systemVersion",
        "userStart": "firstRun",
        "start": "time",
    }
    session = {}
    for name in session_columns:
        session[name] = session_data.get(field_name_mappings.get(name, name))
    session["isDeveloper"] = session_data.get("isDeveloper", False)

    return session

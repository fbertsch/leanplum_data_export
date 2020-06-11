import json
import os


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


def load_session_columns(schema_dir):
    with open(os.path.join(schema_dir, "sessions.schema.json")) as f:
        return [attribute["name"] for attribute in json.load(f)]


def extract_session(session_data, session_columns):
    excluded_columns = {"lat", "lon"}
    session = {}
    for name in session_columns:
        if name not in excluded_columns:
            session[name] = session_data.get(name)
    return session


def main():
    cur_dir = os.path.dirname(__file__)
    schema_dir = os.path.join(cur_dir, "schemas")

    session_columns = load_session_columns(schema_dir)

    with open(os.path.join("sample.ndjson")) as f:
        for line in f:
            session_data = json.loads(line)

            user_attributes = extract_user_attributes(session_data)
            states = extract_states(session_data)
            experiments = extract_experiments(session_data)
            events, event_parameters = extract_events(session_data)
            session = extract_session(session_data, session_columns)

            pass

        print("asfd")


if __name__ == "__main__":
    main()

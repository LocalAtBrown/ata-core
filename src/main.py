import json


def handler(event, context):
    # Note: this is invoked by an event-driven, async method (s3 trigger) so the return value is discarded
    print("request: {}".format(json.dumps(event)))
    return {"hi": "there"}

import datetime as dt
import json

from questdb import Sender
from requests_sse import EventSource

def ingest_change(change, sender):
    symbol_keys = ["type", "server_url", "server_name", "server_script_path", "wiki"]
    other_keys = ["id", "title", "namespace", "comment", "parsedcomment", "user", "bot"]
    sender.row(
        table_name="changes",
        symbols={k: change[k] for k in change if k in symbol_keys},
        columns={k: change[k] for k in change if k in other_keys},
        at=dt.datetime.fromtimestamp(change["timestamp"], dt.timezone.utc)
    )

def stream_changes(stream, sender):
    for event in stream:
        if event.type == "message":
            try:
                change = json.loads(event.data)
            except ValueError as e:
                print(e)
            else:
                if change["meta"]["domain"] == "canary":
                    continue            
                ingest_change(change, sender)

url = "https://stream.wikimedia.org/v2/stream/recentchange"
headers = {"User-Agent": "questdb-wikipedia-ingestion"}
with EventSource(url, headers=headers) as stream:
    conf = "ws::addr=127.0.0.1:9000;"
    with Sender.from_conf(conf) as sender:
        stream_changes(stream, sender)

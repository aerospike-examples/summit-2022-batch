# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as oh
from aerospike_helpers.operations import map_operations as mh
import datetime as dt
import time
import sys

config = {"hosts": [(options.host, options.port)]}
ns = options.namespace
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    print(e)
    sys.exit(1)

policy = {"key": aerospike.POLICY_KEY_SEND}
map_policy = {
    "map_write_mode": aerospike.MAP_UPDATE,
    "map_order": aerospike.MAP_KEY_ORDERED,
}

def to_day(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d")

def to_minute(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d%H%M")

games = [
    "pacman",
    "asteroids",
    "donkey-kong",
    "mortal-kombat",
]

# every minute update the leaderboards based on the unprocessed minute buckets
now = 1651824999
#minutes = []
#for i in range(now, now - 3600, -60):
#    minutes.append(to_minute(i))
batch = []
ops = [oh.read("scores")]
for game in games:
    pk = "{}|{}".format(game, to_minute(now))
    batch.append(br.Read((ns, "minute-scores", pk), ops, policy))
month = pk[-12:-6]
day = pk[-12:-4]
res = client.batch_write(br.BatchRecords(batch))

leaders = []
for i in range(len(batch)):
    game = games[i]
    if res.batch_records[i].result == 0:
        k, m, b = res.batch_records[i].record
        scores = b["scores"]
        ops = [
            mh.map_put_items("scores", scores, map_policy),
            mh.map_remove_by_rank_range(
                "scores", -100, 100, aerospike.MAP_RETURN_NONE, True
            ),
        ]
        print("Update {} top-overall with new top scores".format(game))
        leaders.append(br.Write((ns, "top-overall", game), ops, policy))
        pk = "{}|{}".format(game, month)
        print("Update {} top-monthly with new top scores".format(pk))
        leaders.append(br.Write((ns, "top-monthly", pk), ops, policy))
        pk = "{}|{}".format(game, day)
        print("Update {} top-daily with new top scores".format(pk))
        leaders.append(br.Write((ns, "top-daily", pk), ops, policy))
print("\nRun batch write")
res = client.batch_write(br.BatchRecords(leaders))

client.close()

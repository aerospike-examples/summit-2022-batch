# -*- coding: utf-8 -*-
from args import options
from args import games
import aerospike
from aerospike import exception
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import map_operations as mh
import sys

if options.set == "summit":
    options.set = "users"
ns = options.namespace
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    print(e)
    sys.exit(1)

def print_scores(game, scores):
    print("{}\n=============".format(game))
    hi_to_low = reversed(scores)
    for i in hi_to_low:
        print("user {}: {}".format(next(hi_to_low), i))
    print()

policy = {"key": aerospike.POLICY_KEY_SEND}
map_policy = {
    "map_write_mode": aerospike.MAP_UPDATE,
    "map_order": aerospike.MAP_KEY_ORDERED,
}

users = []
for i in range(100, 110):
    users.append((ns, options.set, "user{}".format(i)))
print("\nPersonal best scores for a batch of users:")
res = client.select_many(users, ["account"])
for record in res:
    k, m, b = record
    user = k[2]
    personal_best = {}
    for game in b["account"]["scores"]:
        personal_best[game] = b["account"]["scores"][game]["top"]
    print("{}: {}".format(user, personal_best))

print("\nGet the top 3 high scores for the games:")
ops = [mh.map_get_by_rank_range("scores", -3, 3, aerospike.MAP_RETURN_KEY_VALUE)]
batch = []
for game in games:
    batch.append(br.Read((ns, "top-overall", game), ops, policy))
res = client.batch_write(br.BatchRecords(batch))
for i in range(len(batch)):
    if res.batch_records[i].result == 0:
        k, m, b = res.batch_records[i].record
        print_scores(k[2], b["scores"])

client.close()

# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import cdt_ctx as ctx
from aerospike_helpers import expressions as exp
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import map_operations as mh
from aerospike_helpers.operations import expression_operations as opexp
import datetime as dt
import random
import string
import time
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

policy = {"key": aerospike.POLICY_KEY_SEND}
map_policy = {
    "map_write_mode": aerospike.MAP_UPDATE,
    "map_order": aerospike.MAP_KEY_ORDERED,
}

def to_day(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d")

def to_minute(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d%H%M")

# new score comes in as the following JSON
new = {
    "game": "pacman",
    "score": 123456,
    "user": "user10",
    "ts": 1651824999
}
key = (ns, options.set, new["user"])

top_value_exp = exp.Cond(
      exp.GT(new["score"], exp.MapGetByKey([ctx.cdt_ctx_map_key("scores"),
          ctx.cdt_ctx_map_key(new["game"])], aerospike.MAP_RETURN_VALUE,
          exp.ResultType.INTEGER, "top", exp.MapBin("account"))),
      exp.MapPut([ctx.cdt_ctx_map_key("scores"),
        ctx.cdt_ctx_map_key(new["game"])], None, "top", new["score"],
        exp.MapBin("account")),
      exp.Unknown()
).compile()

is_highscore_exp = exp.GT(new["score"], exp.MapGetByKey([ctx.cdt_ctx_map_key("scores"),
      ctx.cdt_ctx_map_key(new["game"])], aerospike.MAP_RETURN_VALUE,
      exp.ResultType.INTEGER, "top", exp.MapBin("account"))).compile()

ops = [
    opexp.expression_read("new_top", is_highscore_exp),
    mh.map_put("account", "last-seen", to_day(new["ts"]), map_policy),
    mh.map_put("account", "last-played", to_day(new["ts"]), ctx=[ctx.cdt_ctx_map_key("scores"), ctx.cdt_ctx_map_key(new["game"])]),
    mh.map_increment("account", "games-played", 1, ctx=[ctx.cdt_ctx_map_key("scores"), ctx.cdt_ctx_map_key(new["game"])]),
    opexp.expression_write("account", top_value_exp, aerospike.EXP_WRITE_EVAL_NO_FAIL)
]
_, _, b = client.operate(key, ops, policy=policy)

if b["new_top"]:
    print("This is a new high score for {}".format(new["user"]))
    batch = []
    ops = [
        mh.map_put("account", "achieved", to_day(new["ts"]),
            ctx=[ctx.cdt_ctx_map_key("scores"), ctx.cdt_ctx_map_key(new["game"])])
    ]
    batch.append(br.Write(key, ops, policy))
    pk = "{}|{}".format(new["game"], to_minute(new["ts"]))
    minute_bucket = (ns, "minute-scores", pk)
    ops = [
        mh.map_put("scores", new["user"], new["score"], map_policy)
    ]
    batch.append(br.Write(minute_bucket, ops, policy))
    client.batch_write(br.BatchRecords(batch))
    print("Wrote to score minute bucket ", pk)

client.close()

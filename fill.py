# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import map_operations as mh
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

client.truncate(ns, options.set, 0)
policy = {"key": aerospike.POLICY_KEY_SEND}
map_policy = {
    "map_write_mode": aerospike.MAP_UPDATE,
    "map_order": aerospike.MAP_KEY_ORDERED,
}


def to_day(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d")


def rand_ts(start, end):
    if (end - start) < 3600:
        end = start + 10000
    try:
        return random.randrange(start, end, 3600)
    except Exception as e:
        print(start, end, e)


alnum = string.ascii_uppercase + string.digits
games = [
    "pacman",
    "asteroids",
    "donkey-kong",
    "mortal-kombat",
]
top_overall = {}
top_monthly = {}
top_daily = {}
for game in games:
    top_overall[game] = {}
    top_monthly[game] = {}
    top_daily[game] = {}


def gen_user(pk):
    last_seen = rand_ts(1622530800, 1651733999)  # 20210601 to 20220504
    scores = {}
    for g in range(len(games)):
        game = random.choices(games)[0]
        score = random.randrange(100, 100000)
        last_played = rand_ts(1622530800, last_seen)
        achieved = rand_ts(1622530800, last_played)
        day_achieved = to_day(achieved)
        games_played = random.randrange(1, 900)
        top_overall[game][pk] = score
        try:
            yyyymm = day_achieved[0:6]
            top_monthly[game][yyyymm][pk] = score
        except KeyError:
            top_monthly[game][yyyymm] = {}
            top_monthly[game][yyyymm][pk] = score
        try:
            top_daily[game][day_achieved][pk] = score
        except KeyError:
            top_daily[game][day_achieved] = {}
            top_daily[game][day_achieved][pk] = score
        scores[game] = {
            "top": score,
            "achieved": to_day(achieved),
            "games-played": games_played,
            "last-played": to_day(last_played),
        }
    user = {"last-seen": to_day(last_seen), "scores": scores}
    return user


timer_on = time.perf_counter_ns()
for u in range(2500):
    pk = "user{}".format(u)
    user = gen_user(pk)
    key = (ns, options.set, pk)
    ops = [
        mh.map_put_items("account", user, map_policy),
    ]
    client.operate((ns, options.set, pk), ops, policy=policy)
timer_off = time.perf_counter_ns()
print("Nanoseconds to generate 2.5K records individually", timer_off - timer_on)

# the batch way
timer_on = time.perf_counter_ns()
for b in range(10):
    for u in range(2500):
        batch = []
        pk = "".join(random.choices(string.ascii_lowercase, k=8))
        user = gen_user(pk)
        key = (ns, options.set, pk)
        ops = [
            mh.map_put_items("account", user, map_policy),
        ]
        batch.append(br.Write(key, ops, policy))
    client.batch_write(br.BatchRecords(batch))
timer_off = time.perf_counter_ns()
print("Nanoseconds to batch-write 10 batches of 2.5K records ", timer_off - timer_on)

# fill the top scores
for game in games:
    ops = [
        mh.map_put_items("scores", top_overall[game], map_policy),
        mh.map_remove_by_rank_range(
            "scores", -100, 100, aerospike.MAP_RETURN_NONE, True
        ),
    ]
    client.operate((ns, "top-overall", game), ops, policy=policy)
    for month in top_monthly[game]:
        ops = [
            mh.map_put_items("scores", top_monthly[game][month], map_policy),
            mh.map_remove_by_rank_range(
                "scores", -100, 100, aerospike.MAP_RETURN_NONE, True
            ),
        ]
        pk = "{}|{}".format(game, month)
        client.operate((ns, "top-monthly", pk), ops, policy=policy)
    for day in top_daily[game]:
        ops = [
            mh.map_put_items("scores", top_daily[game][day], map_policy),
        ]
        pk = "{}|{}".format(game, day)
        client.operate((ns, "top-daily", pk), ops, policy=policy)

client.close()

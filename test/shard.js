// @flow

var Sharder = require('../sharder.js');
var test = require('tape');

test('Get shard ID test', function (t) {
    t.plan(5);

    var sharder = new Sharder(60000); // 1min

    var shardId;

    shardId = sharder.getShardIdFromTs(0);
    t.equals(shardId, 0, 'Zero test');

    shardId = sharder.getShardIdFromTs(1);
    t.equals(shardId, 0, 'First shard');

    shardId = sharder.getShardIdFromTs(60001);
    t.equals(shardId, 60000, 'Second Shard');


    shardId = sharder.getShardIdFromTs(-1);
    t.equals(shardId, -60000, 'Negative first shard');


    shardId = sharder.getShardIdFromTs(-60001);
    t.equals(shardId, -120000, 'Negative second shard');

});

test('Validate shard ID test', function (t) {
    t.plan(9);

    var sharder = new Sharder(60000); // 1min

    var result;

    result = sharder.validateShardId(0);
    t.true(result, 'Zero Id')

    result = sharder.validateShardId(60000);
    t.true(result, 'First positive')

    result = sharder.validateShardId(120000);
    t.true(result, 'Second positive')

    result = sharder.validateShardId(-60000);
    t.true(result, 'First negative')

    result = sharder.validateShardId(-120000);
    t.true(result, 'Second negative')

    result = sharder.validateShardId(1);
    t.true(!result, 'First positive error')

    result = sharder.validateShardId(60001);
    t.true(!result, 'Second positive error')

    result = sharder.validateShardId(-1);
    t.true(!result, 'First negative error')

    result = sharder.validateShardId(-60001);
    t.true(!result, 'Second negative error')

});


test('Get range', function (t) {

    t.plan(3);

    var sharder = new Sharder(60000); // 1min

    var result;

    result = sharder.getShardIdsFromRange([0, 120000]);
    t.deepEquals(result, [0, 60000], 'Exclusive test')

    result = sharder.getShardIdsFromRange([0, 120001]);
    t.deepEquals(result, [0, 60000, 120000], 'Inclusive test')

    result = sharder.getShardIdsFromRange([0, 1]);
    t.deepEquals(result, [0], 'Single shard test')

});

// @flow

var DataShard = require('../index.js');
var test = require('tape');

test('General Test', function (t) {
    t.plan(4);

    var dataShard = new DataShard(60000, fetchFunc);

    function printTime(time) {
        console.log('========================================');
        console.log('Time: ' + time);
        console.log('========================================');
    }

    printTime(0);

    try {
        dataShard.hasDataForShardId(12, 0);
        console.log('Failure 1!');
    } catch (err) {
        console.log(err);
        console.log('Success 1!');
    }

    console.log('hasDataForShardId for shardId 60000 for tier 0: ');
    console.log(dataShard.hasDataForShardId(60000));


    dataShard.fetchDataForShardId(0);
    dataShard.fetchMissingDataForRange([0, 60001]);

    console.log('Missing shardIds: ');
    console.log(dataShard.computeMissingShardIdsFromRange([0, 60001]));

    setTimeout(function() {
        printTime(50);
        console.log('dataShard: ');
        console.log(dataShard);
    }, 50);

    setTimeout(function() {
        printTime(200);
        console.log('dataShard: ');
        console.log(JSON.stringify(dataShard, null, 4));
    }, 200);

    setTimeout(function() {
        printTime(600);
        console.log('Missing shardIds: ');
        var missing = dataShard.computeMissingShardIdsFromRange([0, 180001]);
        console.log(missing);
        var expected = [ 120000, 180000 ];
        t.deepEquals(expected, missing);
    }, 600);

    setTimeout(function() {
        printTime(700);
        console.log('Data in range: ');
        var data = dataShard.getDataForRange([0, 800000]);
        console.log(data);

        var expected = [ { time: 0, value: 0 },
            { time: 59999, value: 59999 },
            { time: 60000, value: 60000 },
            { time: 119999, value: 119999 } ];

        t.deepEquals(data, expected);
    }, 700);

    setTimeout(function() {
        printTime(800);
        dataShard.addDataPoint({
            time: 110000,
            value: 111
        });
        dataShard.addDataPoint({
            time: 120000,
            value: 222
        });
        var data;
        console.log('Data in for shard id 60000: ');
        data = dataShard.getDataForShardId(60000);
        t.deepEquals(data.length, 3);
        console.log(data);
        console.log('Data in for shard id 120000: ');
        data = dataShard.getDataForShardId(120000);
        console.log(data);
        t.deepEquals(data.length, 1);
    }, 800);

    function fetchFunc(range, cb) {
        setTimeout(
            function() {
                cb(
                    null,
                    [
                        {
                            time: range[0],
                            value: range[0]
                        },
                        {
                            time: range[1]-1,
                            value: range[1]-1
                        }
                    ]
                );
            },
            100
        );
    }

});

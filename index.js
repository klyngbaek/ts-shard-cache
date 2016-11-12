//@flow

"use strict";

var Sharder = require('./sharder');

class TsShardCache {

    /*::_sharder: Sharder*/
    /*::_shardSize: ShardSizeType*/
    /*::_data: {[key: ShardIdType]: Array<DataPoint>}*/
    /*::_pendingReqs: {[key: ShardIdType]: number}*/
    /*::_fetchFunc: FetchFuncType*/
    /*::_handleFetch: ?HandleFetchFuncType*/
    /*::_timeout: number*/

    constructor(shardSize/*:ShardSizeType*/, fetchFunc/*:FetchFuncType*/, handleFetch/*:?HandleFetchFuncType*/)/*:void*/ {
        this._sharder = new Sharder(shardSize);
        this._shardSize = shardSize;
        this._data = {};
        this._pendingReqs = {};
        this._fetchFunc = fetchFunc;
        this._handleFetch = handleFetch;
        this._timeout = 5000;
    }

    // fetch

    fetchMissingDataForRange(range/*:RangeType*/)/*:void*/ {
        var missingShardIds = this.computeMissingShardIdsFromRange(range);
        this.fetchDataForShardIds(missingShardIds);
    }

    fetchDataForShardIds(shardIds/*:Array<ShardIdType>*/)/*:void*/ {
        var self = this;
        shardIds.forEach(function(shardId/*:ShardIdType*/)/*:void*/ {
            if (!self.hasPendingReqForShardId(shardId)) {
                self.fetchDataForShardId(shardId);
            }
            else {
                console.log('Already requested: ' + shardId);
            }
        });
    }

    fetchDataForShardId(shardId/*:ShardIdType*/)/*:void*/ {
        if (this.hasDataForShardId(shardId)) throw new Error('Already have data for this shardId ' + shardId);
        if (this.hasPendingReqForShardId(shardId)) throw new Error('Already have pending req for shardId!' + shardId);

        var self = this;

        // keep a timer to make sure requests are done in a timely manner
        var timeoutId = setTimeout(function(){handleTimeout(shardId);}, self._timeout);
        self._pendingReqs[shardId] = timeoutId;
        var range = self.computeRangeFromShardId(shardId);

        this._fetchFunc(range, function(err/*:?Error*/, data/*:?Array<DataPoint>*/)/*:void*/ {

            clearTimeout(self._pendingReqs[shardId]);
            delete self._pendingReqs[shardId];
            if (err) {
                // there was an error, do not populate data
                console.error(err);
            }
            else if (data) {
                if (self.validateShardData(shardId, data)) {
                    // success! populate data
                    self.setDataForShardId(shardId, data);
                }
                else {
                    // returned shard data was not valid,
                    // log error and do no populate array
                    console.error('shardData not valid ' + shardId);
                }
            } else {
                console.error('shardData not valid ' + shardId);
            }
        });

        function handleTimeout(shardId/*:ShardIdType*/)/*:void*/ {
            console.warn('Request to get data for this range took took long: ' + shardId);
        }
    }

    // set

    setDataForShardId(shardId/*:ShardIdType*/, data/*:Array<DataPoint>*/)/*:void*/ {
        this._data[shardId] = data;
    }

    // add DataPoint

    addDataPoint(dataPoint/*:DataPoint*/)/*:void*/ {
        var shardId = this._sharder.getShardIdFromTs(dataPoint.time);
        if (!this.hasDataForShardId(shardId)) {
            this._data[shardId] = [];
        }
        var arr = this._data[shardId];
        var indexToInsert = sortedIndex(arr, dataPoint);
        insert(arr, indexToInsert, dataPoint);
    }

    // get

    getDataForShardId(shardId/*:ShardIdType*/)/*:Array<DataPoint>*/ {
        return this._data[shardId];
    }

    getDataForShardIds(shardIds/*:Array<ShardIdType>*/)/*:Array<DataPoint>*/ {
        var result = [];
        var self = this;
        shardIds.forEach(function(shardId/*:ShardIdType*/)/*:void*/ {
            if (self.hasDataForShardId(shardId)) {
                var shardData = self._data[shardId];
                result = result.concat(shardData);
            }
        });
        return result;
    }

    getDataForRange(range/*:RangeType*/)/*:Array<DataPoint>*/ {
        var shardIds = this.computeShardIdsFromRange(range);
        return this.getDataForShardIds(shardIds);
    }

    // compute

    computeMissingShardIdsFromShardIds(shardIds/*:Array<ShardIdType>*/)/*:Array<ShardIdType>*/ {
        var self = this;
        var result = [];
        shardIds.forEach(function(shardId/*:ShardIdType*/)/*:void*/ {
            if (!self.hasDataForShardId(shardId)) {
                result.push(shardId);
            }
        });
        return result;
    }

    computeMissingShardIdsFromRange(range/*:RangeType*/)/*:Array<ShardIdType>*/ {
        var self = this;
        var shardIds = this.computeShardIdsFromRange(range);
        return self.computeMissingShardIdsFromShardIds(shardIds);
    }

    computeRangeFromShardId(shardId/*:ShardIdType*/)/*:RangeType*/ {
        if (!this.validateShardId(shardId)) throw new Error('Invalid shardId ' + shardId);
        var shardSize = this._shardSize;
        return [shardId, shardId+shardSize];
    }

    computeShardIdsFromRange(range/*:RangeType*/)/*:Array<ShardIdType>*/ {
        if (!validateRange) throw new Error('Invalid range ' + range.toString());

        var startTime = range[0];
        var endTime = range[1];

        var shardSize = this._shardSize;
        var startShardId = startTime - startTime%shardSize;
        var result = [];
        var curShardId = startShardId;

        while (curShardId < endTime) {
            result.push(curShardId);
            curShardId += shardSize;
        }

        return result;
    }

    // has

    hasDataForShardId(shardId/*:ShardIdType*/)/*:boolean*/ {
        if (!this.validateShardId(shardId)) throw new Error('Invalid shardId ' + shardId);
        return (!!this._data[shardId]);
    }

    hasPendingReqForShardId(shardId/*:ShardIdType*/)/*:boolean*/ {
        return (!!this._pendingReqs[shardId]);
    }

    // validate

    validateShardData(shardId/*:ShardIdType*/, shardData/*:Array<DataPoint>*/)/*:ValidationType*/ {
        if (!this.validateShardId(shardId)) throw new Error('Invalid shardId ' + shardId);

        var isValid = true;

        var lastTime = Number.NEGATIVE_INFINITY;
        shardData.every(function(dataPoint/*:DataPoint*/)/*:boolean*/ {
            if (typeof dataPoint.time !== 'number') isValid = false;
            if (typeof dataPoint.value !== 'number') isValid = false;
            if (dataPoint.time <= lastTime) isValid = false;
            lastTime = dataPoint.time;
            return isValid;
        });

        var range = this.computeRangeFromShardId(shardId);
        var startTime = range[0];
        var endTime = range[1];
        if (shardData.length > 0) {
            var firstDataPoint = shardData[0];
            var lastDataPoint = shardData[shardData.length-1];
            if (firstDataPoint.time < startTime) isValid = false;
            if (lastDataPoint.time >= endTime) isValid = false;
        }

        return isValid;
    }

    validateShardId(shardId/*:ShardIdType*/)/*:ValidationType*/ {
        var shardSize = this._shardSize;
        return shardId % shardSize === 0;
    }

}

// validate

function validateRange(range/*:RangeType*/)/*:ValidationType*/ {
    if (range.length!==2) return false;
    var startTime = range[0];
    var endTime = range[1];
    if (startTime>=endTime) return false;
    return true;
}

function insert(arr/*Array<DataPoint>*/, index/*:number*/, item/*:DataPoint*/)/*:void*/ {
    arr.splice(index, 0, item);
}

function sortedIndex(array/*Array<DataPoint>*/, value/*Array<DataPoint>*/)/*:number*/ {
    var low = 0,
        high = array.length;

    while (low < high) {
        var mid = low + high >>> 1;
        if (array[mid].time < value.time) low = mid + 1;
        else high = mid;
    }
    return low;
}

module.exports = TsShardCache;

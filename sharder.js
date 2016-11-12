//@flow

"use strict";

class Sharder {

    /*::_shardSize: ShardSizeType*/

	constructor(shardSize/*:ShardSizeType*/) {
		if (!Number.isSafeInteger(shardSize)) throw new Error('Invalid chunk size, must be an integer.');
		if (shardSize < 0) throw new Error('Invalid chunk size, must be greater than 0.');
		this._shardSize = shardSize;
	}

	getShardIdFromTs(ts/*:TsType*/) {
		if (!validateTs(ts)) throw new Error('Invalid arg ts. Not a number: ' + ts);
		var rmdr = ts%this._shardSize;
		if (rmdr === 0) return ts - rmdr;
		else if (ts > 0) return ts - rmdr;
		else if (ts < 0) return ts - (this._shardSize + rmdr);
		else throw new Error('Something is very wrong');
	};

	getShardIdsFromRange(range/*:RangeType*/) {
		var startTs = range[0];
		var endTs = range[1];
		if ( startTs >= endTs ) return [];
		var startShardId = this.getShardIdFromTs(startTs);
		var result = [];
		var curShardId = startShardId;
		while (curShardId < endTs) {
			result.push(curShardId);
			curShardId += this._shardSize;
		}
		return result;
	};

	validateShardId(shardId/*:ShardIdType*/) {
		return shardId%this._shardSize === 0;
	};

}

function validateTs(ts/*:TsType*/) {
	var errors = [];
	if (!Number.isSafeInteger(ts)) {
		errors.push('ts is not a safe integer' + ts);
	}
	return (errors.length===0);
}

module.exports = Sharder;

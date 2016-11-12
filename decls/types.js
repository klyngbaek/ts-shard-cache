declare type ValueType = number

declare type TsType = number

declare type ShardIdType = number

declare type TierType = number

declare type ShardSizeType = number

declare type ValidationType = boolean

declare type RangeType = Array<TsType>

declare type TiersType = Array<ShardSizeType>

declare type DataPoint = {
  time: TsType;
  value: ValueType;
}

declare type HandleFetchFuncType = (shardId: ShardIdType, tier: TierType) => void

declare type FetchFuncType = (range: RangeType, cb: FetchCallbackType) => void

declare type FetchCallbackType = (err: ?Error, data: ?Array<DataPoint>) => void

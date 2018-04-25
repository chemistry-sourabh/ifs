package ifs

const AttrRequest = 0
const ReadDirRequest = 1
const FetchFileRequest = 2
const ReadFileRequest = 3
const WriteFileRequest = 4
const SetAttrRequest = 5
const CreateRequest = 6
const RemoveRequest = 7
const RenameRequest = 8

const CacheFileRequest = 10
const CacheCreateRequest = 11
const CacheWriteRequest = 12
const CacheDeleteRequest = 13
const CacheSyncRequest = 14
const CacheTruncRequest = 15
const CacheRenameRequest = 16

const StatResponse = 20
const StatsResponse = 21
const FileDataResponse = 22
const WriteResponse = 23
const ErrorResponse = 24

const ChannelLength = 100

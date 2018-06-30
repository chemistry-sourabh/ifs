package ifs

const RequestBase = 0
const AttrRequest = RequestBase + 0
const ReadDirRequest = RequestBase + 1
const FetchFileRequest = RequestBase + 2
const ReadFileRequest = RequestBase + 3
const WriteFileRequest = RequestBase + 4
const SetAttrRequest = RequestBase + 5
const CreateRequest = RequestBase + 6
const RemoveRequest = RequestBase + 7
const RenameRequest = RequestBase + 8
const OpenRequest = RequestBase + 9
const CloseRequest = RequestBase + 10
const FlushRequest  = RequestBase + 11
const ReadDirAllRequest = RequestBase + 12

//const CacheBase = 20
//const CacheFileRequest = CacheBase + 0
//const CacheCreateRequest = CacheBase + 1
//const CacheWriteRequest = CacheBase + 2
//const CacheDeleteRequest = CacheBase + 3
//const CacheTruncRequest = CacheBase + 4
//const CacheRenameRequest = CacheBase + 5
//const CacheOpenRequest = CacheBase + 6
//const CacheSyncRequest = CacheBase + 7

const ResponseBase = 30
const StatResponse = ResponseBase + 0
const StatsResponse = ResponseBase + 1
const FileDataResponse = ResponseBase + 2
const WriteResponse = ResponseBase + 3
const ErrorResponse = ResponseBase + 4

const ChannelLength = 100

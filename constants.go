package ifs

const FileOpBase = 0
const AttrRequest = FileOpBase + 0
const ReadDirRequest = FileOpBase + 1
const FetchFileRequest = FileOpBase + 2
const ReadFileRequest = FileOpBase + 3
const WriteFileRequest = FileOpBase + 4
const SetAttrRequest = FileOpBase + 5
const CreateRequest = FileOpBase + 6
const RemoveRequest = FileOpBase + 7
const RenameRequest = FileOpBase + 8
const OpenRequest = FileOpBase + 9
const CloseRequest = FileOpBase + 10
const FlushRequest  = FileOpBase + 11
const ReadDirAllRequest = FileOpBase + 12

const WatcherBase = 20
const WatchDirRequest = WatcherBase + 0
const AttrUpdateRequest = WatcherBase + 1

const ResponseBase = 30
const StatResponse = ResponseBase + 0
const StatsResponse = ResponseBase + 1
const FileDataResponse = ResponseBase + 2
const WriteResponse = ResponseBase + 3
const ErrorResponse = ResponseBase + 4

const ChannelLength = 100

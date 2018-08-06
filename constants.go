/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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

const ResponseBase = 30
const StatResponse = ResponseBase + 0
const StatsResponse = ResponseBase + 1
const FileDataResponse = ResponseBase + 2
const WriteResponse = ResponseBase + 3
const ErrorResponse = ResponseBase + 4

const ChannelLength = 100

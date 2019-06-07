/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package structure

const RequestCodeBase = 0
const FetchMessageCode = RequestCodeBase + 1
const RenameMessageCode = RequestCodeBase + 2
const OpenMessageCode = RequestCodeBase + 3
const CreateMessageCode = RequestCodeBase + 4
const RemoveMessageCode = RequestCodeBase + 5
const CloseMessageCode = RequestCodeBase + 6
const TruncateMessageCode = RequestCodeBase + 7
const FlushMessageCode = RequestCodeBase + 8
const ReadMessageCode = RequestCodeBase + 9
const WriteMessageCode = RequestCodeBase + 10
const AttrMessageCode = RequestCodeBase + 11
const ReadDirMessageCode = RequestCodeBase + 12

const ReplyCodeBase = 100
const OkMessageCode = ReplyCodeBase + 1
const ErrMessageCode = ReplyCodeBase + 2
const DataMessageCode = ReplyCodeBase + 3
const WriteOkMessageCode = ReplyCodeBase + 4
const FileInfoMessageCode = ReplyCodeBase + 5
const FileInfosMessageCode = ReplyCodeBase + 6

const ChannelLength = 1000

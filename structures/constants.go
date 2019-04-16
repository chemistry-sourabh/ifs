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

package structures

const MessageCodeBase = 0
const FetchMessageCode = MessageCodeBase + 1
const FileMessageCode = MessageCodeBase + 2

const ResponseBase = 30
const StatResponse = ResponseBase + 0
const StatsResponse = ResponseBase + 1
const FileDataResponse = ResponseBase + 2
const WriteResponse = ResponseBase + 3
const ErrorResponse = ResponseBase + 4

const ChannelLength = 100

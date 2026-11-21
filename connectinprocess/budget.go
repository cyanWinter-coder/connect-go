// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connectinprocess

import (
	"connectrpc.com/connect/v2"
	"google.golang.org/protobuf/proto"
)

// messageSize reports the serialized size of msg for stream budgeting. The
// in-process transport exchanges no wire framing, so [proto.Message]-shaped
// values size themselves with proto.Size; other messages report zero and
// are admitted without consuming the byte window.
func messageSize(msg any) int {
	if pm, ok := msg.(proto.Message); ok {
		if size := proto.Size(pm); size > 0 {
			return size
		}
	}
	if sizer, ok := msg.(connect.MessageSizer); ok {
		if size := sizer.ProtoSize(); size > 0 {
			return size
		}
	}
	return 0
}

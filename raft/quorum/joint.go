// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quorum

import (
	"math"
)

// JointConfig is a configuration of two groups of (possibly overlapping)
// majority configurations. Decisions require the support of both majorities.
type JointConfig [6]MajorityConfig

func (c JointConfig) String() string {
	if len(c[1]) > 0 {
		str := ""
		for _, mc := range c {
			str += mc.String() + "&&"
		}
		return str[:len(str)-2]
	}
	return c[0].String()
}

// IDs returns a newly initialized map representing the set of voters present
// in the joint configuration.
func (c JointConfig) IDs() map[uint64]struct{} {
	m := map[uint64]struct{}{}
	for _, cc := range c {
		for id := range cc {
			m[id] = struct{}{}
		}
	}
	return m
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
func (c JointConfig) Describe(l AckedIndexer) string {
	return MajorityConfig(c.IDs()).Describe(l)
}

// CommittedIndex returns the largest committed index for the given joint
// quorum. An index is jointly committed if it is committed in both constituent
// majorities.
func (c JointConfig) CommittedIndex(l AckedIndexer, quorum uint64) Index {
	min := uint64(math.MaxUint64)
	for _, mc := range c {
		ci := uint64(mc.CommittedIndex(l, quorum))
		if ci < min {
			min = ci
		}
	}
	return Index(min)
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending, lost, or won. A joint quorum
// requires both majority quorums to vote in favor.
func (c JointConfig) VoteResult(votes map[uint64]bool, quorum uint64) VoteResult {
	//log.Print(c)
	if len(c[1]) > 0 {
		//log.Print("raft/joint.go: setting quorum")
		quorum = uint64(len(c[0]) - len(c[1]))
		//log.Print("raft/joint.go: ReCraft config: ", c[0], "with n = ", quorum)
		return c[0].VoteResult(votes, quorum)
	} else {
		return c[0].VoteResult(votes, quorum)
	}
}

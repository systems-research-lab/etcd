package measure

import (
	"encoding/json"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const MeasureKey = "measurement"

type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Time(t).UnixMicro(), 10)), nil
}

func (t Time) IsZero() bool {
	return time.Time(t).IsZero()
}

type Measure struct {
	SplitEnter Time `json:"splitEnter"`
	SplitLeave Time `json:"splitLeave"`

	MergeTxIssue     Time `json:"mergeTxIssue"`
	MergeTxStart     Time `json:"mergeTxStart"`
	MergeTxCommit    Time `json:"mergeTxCommit"`
	MergeEnter       Time `json:"mergeEnter"`
	MergeSnapReceive Time `json:"mergeSnapReceive"` // timestamp on received all snapshots
	MergeSnapInstall Time `json:"mergeSnapInstall"` // timestamp on installed all snapshots
	MergeLeave       Time `json:"mergeLeave"`

	LeaderElect Time `json:"leaderElect"` // timestamp on leader elected in the new epoch
}

var (
	measurement = Measure{}
	update      = make(chan Measure)
	fetch       = make(chan Measure, 4)
	logger      *zap.Logger
)

func Start(lg *zap.Logger) {
	logger = lg
	logger.Debug("measure started!")

	go func() {
		for {
			select {
			case m := <-update:
				switch {
				case !m.SplitEnter.IsZero():
					measurement.SplitEnter = m.SplitEnter
				case !m.SplitLeave.IsZero():
					measurement.SplitLeave = m.SplitLeave
				case !m.MergeTxIssue.IsZero():
					measurement.MergeTxIssue = m.MergeTxIssue
				case !m.MergeTxStart.IsZero():
					measurement.MergeTxStart = m.MergeTxStart
				case !m.MergeTxCommit.IsZero():
					measurement.MergeTxCommit = m.MergeTxCommit
				case !m.MergeEnter.IsZero():
					measurement.MergeEnter = m.MergeEnter
				case !m.MergeSnapReceive.IsZero():
					measurement.MergeSnapReceive = m.MergeSnapReceive
				case !m.MergeSnapInstall.IsZero():
					measurement.MergeSnapInstall = m.MergeSnapInstall
				case !m.MergeLeave.IsZero():
					measurement.MergeLeave = m.MergeLeave
				case !m.LeaderElect.IsZero():
					measurement.LeaderElect = m.LeaderElect
				default:
					break
				}

				logger.Debug("measurement updated", zap.Any("measurement", measurement))

			case <-fetch:
				fetch <- measurement
			}
		}
	}()
}

func Update() chan<- Measure {
	return update
}

func Json() []byte {
	fetch <- Measure{}
	data, err := json.Marshal(<-fetch)
	if err != nil {
		data = []byte("marshal measurement failed: " + err.Error())
	}
	return data
}

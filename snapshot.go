package elhabash

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/astaxie/beego/logs"

	"github.com/gadelkareem/go-helpers"
	"github.com/vmihailenco/msgpack"
)

const TakeSnapshotCycle = 1000

type SnapshotFile struct {
	sync.Mutex
	path string
}

type Snapshot struct {
	uniqueUrlsMu sync.RWMutex
	uniqueUrls   map[string]bool

	commandsInQueueMu sync.RWMutex
	commandsInQueue   map[string]Cmd

	takenAtMu    sync.RWMutex
	takenAt      time.Time
	disableFile  bool
	snapshotFile *SnapshotFile
}

type DecodableSnapshot struct {
	U map[string]bool
	C map[string]Cmd
}

func NewSnapshot(queue *Queue, name string, disableFile bool) *Snapshot {
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	h.PanicOnError(err)
	path += "/snapshot-" + name + ".dump"

	s := &Snapshot{
		snapshotFile:    &SnapshotFile{path: path},
		uniqueUrls:      make(map[string]bool),
		commandsInQueue: make(map[string]Cmd),
		takenAt:         time.Now(),
		disableFile:     disableFile,
	}

	s.loadSnapshot(queue)

	return s
}

func (s *Snapshot) loadSnapshot(queue *Queue) {
	if s.disableFile {
		return
	}
	ds := s.snapshotFile.load()
	if ds == nil {
		return
	}
	s.uniqueUrls = ds.U
	go time.AfterFunc(2*time.Second, func() {
		for _, c := range ds.C {
			queue.Send(&Cmd{U: c.U, M: c.M, DisableMirror: c.DisableMirror})
		}
		ds.C = nil
	})

	logs.Alert("Found snapshot with %d uniqueUrls and %d commandsInQueue", len(s.uniqueUrls), len(ds.C))
	return
}

func (s *Snapshot) addCommandInQueue(id string, command Command) bool {
	//logs.Debug("Adding command %s", id)
	s.commandsInQueueMu.Lock()
	defer s.commandsInQueueMu.Unlock()
	if _, exists := s.commandsInQueue[id]; exists {
		return false
	}
	s.commandsInQueue[id] = Cmd{U: command.Url(), M: command.Method(), DisableMirror: command.isDisableMirror()}
	return true
}

func (s *Snapshot) removeCommandInQueue(id string) {
	//logs.Debug("Removing command %s", id)
	s.commandsInQueueMu.Lock()
	delete(s.commandsInQueue, id)
	s.commandsInQueueMu.Unlock()
}

func (s *Snapshot) commandInQueue(id string) bool {
	s.commandsInQueueMu.RLock()
	defer s.commandsInQueueMu.RUnlock()
	_, exists := s.commandsInQueue[id]
	return exists
}

func (s *Snapshot) queueLength() int {
	s.commandsInQueueMu.RLock()
	defer s.commandsInQueueMu.RUnlock()
	return len(s.commandsInQueue)
}

func (s *Snapshot) addUniqueUrl(id string) {
	s.uniqueUrlsMu.Lock()
	s.uniqueUrls[id] = true
	urlCount := len(s.uniqueUrls)
	s.uniqueUrlsMu.Unlock()
	if (urlCount % TakeSnapshotCycle) == 0 {
		logs.Debug("Found %d urls so far. Taking a snapshot.. ", urlCount)
		s.takeSnapshot()
	}
}

func (s *Snapshot) uniqueUrlExists(id string) bool {
	s.uniqueUrlsMu.RLock()
	defer s.uniqueUrlsMu.RUnlock()
	_, exists := s.uniqueUrls[id]
	return exists
}

func (s *Snapshot) uniqueUrlsLength() int {
	s.uniqueUrlsMu.RLock()
	defer s.uniqueUrlsMu.RUnlock()
	return len(s.uniqueUrls)
}

func (s *Snapshot) canTakeSnapshot() bool {
	if s.disableFile {
		return false
	}
	s.takenAtMu.Lock()
	defer s.takenAtMu.Unlock()
	if s.takenAt.Add(5 * time.Minute).After(time.Now()) {
		return false
	}
	s.takenAt = time.Now()
	return true
}

func (s *Snapshot) takeSnapshot() {

	if !s.canTakeSnapshot() {
		return
	}

	s.uniqueUrlsMu.RLock()
	s.commandsInQueueMu.RLock()
	ds := DecodableSnapshot{U: s.uniqueUrls, C: s.commandsInQueue}
	logs.Debug("Writing snapshot with %d uniqueUrls and %d commandsInQueue", len(ds.U), len(ds.C))
	data, err := msgpack.Marshal(ds)
	s.uniqueUrlsMu.RUnlock()
	s.commandsInQueueMu.RUnlock()
	if err != nil {
		logs.Error("Message pack Error: %v", err)
		return
	}

	s.snapshotFile.save(data)
}

func (sf *SnapshotFile) save(data []byte) {
	sf.Lock()
	defer sf.Unlock()
	file, err := os.OpenFile(sf.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		logs.Error("Error writing snapshot: %s", err)
		return
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		logs.Error("Error writing file for Mongo Id Store: %s", err)
		return
	}
	logs.Debug("New snapshot written to disk")
	return
}

func (sf *SnapshotFile) load() *DecodableSnapshot {
	sf.Lock()
	defer sf.Unlock()
	file, err := os.Open(sf.path)
	if err != nil {
		logs.Error("Error with snapshot file : %s", err)
		return nil
	}
	defer file.Close()
	logs.Debug("Found snapshot reading..")
	data, err := ioutil.ReadAll(file)
	if err != nil {
		logs.Error("Error reading snapshot file %s", err)
		return nil
	}
	ds := &DecodableSnapshot{
		U: make(map[string]bool),
		C: make(map[string]Cmd),
	}
	err = msgpack.Unmarshal(data, ds)
	if err != nil {
		logs.Error("Error unpacking snapshot %s", err)
		return nil
	}
	return ds
}

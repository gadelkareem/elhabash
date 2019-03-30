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
	doneUrlsMu sync.RWMutex
	doneUrls   map[string]bool

	commandsMu sync.RWMutex
	commands   map[string]Cmd

	takenAtMu    sync.RWMutex
	takenAt      time.Time
	disableFile  bool
	snapshotFile *SnapshotFile
}

type DecodableSnapshot struct {
	U map[string]bool
	C map[string]Cmd
}

func NewSnapshot(name string, disableFile bool) *Snapshot {
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	h.PanicOnError(err)
	path += "/snapshot-" + name + ".dump"

	s := &Snapshot{
		snapshotFile: &SnapshotFile{path: path},
		doneUrls:     make(map[string]bool),
		commands:     make(map[string]Cmd),
		takenAt:      time.Now(),
		disableFile:  disableFile,
	}

	s.loadSnapshot()

	return s
}

func (s *Snapshot) loadSnapshot() {
	if s.disableFile {
		return
	}
	ds := s.snapshotFile.load()
	if ds == nil {
		return
	}
	s.doneUrls = *&ds.U
	s.commands = *&ds.C

	logs.Alert("Found snapshot with %d doneUrls and %d commands", len(s.doneUrls), len(ds.C))
	ds = nil
	return
}

func (s *Snapshot) addCommand(id string, command Command) {
	//logs.Debug("Adding command %s", id)
	s.commandsMu.Lock()
	defer s.commandsMu.Unlock()
	s.commands[id] = Cmd{U: command.Url(), M: command.Method(), DisableMirror: command.isDisableMirror()}
}

func (s *Snapshot) removeCommand(id string) {
	//logs.Debug("Removing command %s", id)
	s.commandsMu.Lock()
	delete(s.commands, id)
	s.commandsMu.Unlock()
}

func (s *Snapshot) commandExists(id string) bool {
	s.commandsMu.RLock()
	defer s.commandsMu.RUnlock()
	_, exists := s.commands[id]
	return exists
}

func (s *Snapshot) totalCommands() int {
	s.commandsMu.RLock()
	defer s.commandsMu.RUnlock()
	return len(s.commands)
}

func (s *Snapshot) addDoneUrl(id string) {
	s.doneUrlsMu.Lock()
	s.doneUrls[id] = true
	urlCount := len(s.doneUrls)
	s.doneUrlsMu.Unlock()
	if (urlCount % TakeSnapshotCycle) == 0 {
		s.takeSnapshot()
	}
}

func (s *Snapshot) doneUrlExists(id string) bool {
	s.doneUrlsMu.RLock()
	defer s.doneUrlsMu.RUnlock()
	_, exists := s.doneUrls[id]
	return exists
}

func (s *Snapshot) doneUrlsLength() int {
	s.doneUrlsMu.RLock()
	defer s.doneUrlsMu.RUnlock()
	return len(s.doneUrls)
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

	s.doneUrlsMu.RLock()
	s.commandsMu.RLock()
	ds := DecodableSnapshot{U: s.doneUrls, C: s.commands}
	logs.Debug("Writing snapshot with %d doneUrls and %d commands", len(ds.U), len(ds.C))
	data, err := msgpack.Marshal(ds)
	s.doneUrlsMu.RUnlock()
	s.commandsMu.RUnlock()
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

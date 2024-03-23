package raft

type LogEntry struct {
	Term    int
	Index   int
	Command any
}

type LogStorage struct {
	entries []*LogEntry
}

func (l *LogStorage) Len() int {
	return len(l.entries)
}

func (l *LogStorage) Append(entry ...*LogEntry) {
	l.entries = append(l.entries, entry...)
}

func (l *LogStorage) DeleteFrom(index int) {
	l.entries = l.entries[:index]
}

func (l *LogStorage) Get(index int) *LogEntry {
	return l.entries[index]
}

func (l *LogStorage) GetRange(from int, to int) []*LogEntry {
	return l.entries[from:to]
}

func (l *LogStorage) First() *LogEntry {
	return l.entries[0]
}

func (l *LogStorage) FirstIndex() int {
	return l.entries[0].Index
}

func (l *LogStorage) Last() *LogEntry {
	return l.entries[len(l.entries)-1]
}

func (l *LogStorage) LastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

func (l *LogStorage) FindFirstByTerm(term int) *LogEntry {
	for _, entry := range l.entries {
		if entry.Term > term {
			return nil
		}
		if entry.Term == term {
			return entry
		}
	}
	return nil
}

func (l *LogStorage) FindLastByTerm(term int) *LogEntry {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term < term {
			return nil
		}
		if l.entries[i].Term == term {
			return l.entries[i]
		}
	}
	return nil
}

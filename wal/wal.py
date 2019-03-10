class LogEntry :
    def __init__(self, log_id, parent, data) :
        self.log_id = log_id
        self.parent_log_id = parent
        self.data = data

class WALCommitError(Exception):
    pass

class WALPruneError(Exception):
    pass

class WALAppendError(Exception):
    pass

class WAL :
    def __init__(self) :
        self.heads = {}
        self.commit_id = None
        self.reverse_dependencies = {}

    def commit(self, *args, **kwargs) :
        return self._commit(*args, **kwargs)

    def _commit(self, log_id, auto_prune=False) :
        entry = self.heads[log_id]

        if entry.parent_log_id != self.commit_id :
            raise CommitError(f"Can't fast forward to {log_id},"
                    " its parent is not HEAD")

        assert log_id in self.reverse_dependencies[self.commit_id]
        if not auto_prune :
            if len(self.reverse_dependencies[commit_id]) > 1 :
                raise CommitError(f"Can't commit {log_id}, {self.commit_id}"
                        " has unpruned reverse dependencies.")
        else :
            for x in self.reverse_dependencies[commit_id] :
                if log_id != x :
                    self.prune(x, strict=False)

        del self.reverse_dependencies[self.commit_id]

        self.commit_id = log_id

        del self.heads[log_id]

        return entry

    def prune(self, log_id, strict=True) :
        if strict :
            if len(self.reverse_dependencies[log_id]) > 0 :
                raise PruneError(f"Can't prune {log_id},"
                        " has reverse dependencies")
        else :
            for x in self.reverse_dependencies[log_id] :
                self.prune(x.log_id, strict=False)

        entry = self.heads[log_id]

        rd = self.reverse_dependencies[entry.parent_log_id]
        # if rd were a dict this would be guaranteed O(1)
        if log_id in rd :
            rd.remove(log_id)

        del self.heads[log_id]
        del self.reverse_dependencies[log_id]

    def append_log_entry(self, entry) :
        if entry.log_id in self.heads :
            raise AppendError(f'{log_id} already in WAL')
        if self.commit_id != entry.parent_log_id :
            if entry.parent_log_id not in self.heads :
                raise AppendError(f'Parent {parent_log_id} not in WAL')

        self.reverse_dependencies[entry.parent_log_id].append(entry.log_id)

        self.reverse_dependencies[entry.log_id] = []
        self.heads[entry.log_id] = entry

"""Hardware abstraction layer for the system on which process groups are run.

Classes:
Partition -- atomic set of nodes
PartitionDict -- default container for partitions
ProcessGroup -- a group of processes started with mpirun
ProcessGroupDict -- default container for ProcessGroup instances
BGSystem -- Blue Gene system component

Exceptions:
ProcessGroupCreationError -- error when creating a ProcessGroup
"""

import atexit
import pwd
import sets
import logging
import sys
import os
import operator
import random
import signal
import tempfile
import time
import thread
import ConfigParser
import traceback
import thread
from datetime import datetime
try:
    set = set
except NameError:
    from sets import Set as set

import Cobalt
import Cobalt.Data
from Cobalt.Data import Data, DataDict, DataList, IncrID, DataCreationError
from Cobalt.Components.base import Component, exposed, automatic, query
import Cobalt.bridge
from Cobalt.bridge import BridgeException

__all__ = [
    "ProcessGroupCreationError",
    "Partition", "PartitionDict",
    "ProcessGroup", "ProcessGroupDict",
    "Simulator",
]

logger = logging.getLogger(__name__)
Cobalt.bridge.set_serial("BGP")

class ProcessGroupCreationError (Exception):
    """An error occured when creation a process group."""


class NodeCard (object):
    def __init__(self, name):
        self.id = name
        self.used_by = ''
        
    def __eq__(self, other):
        return self.id == other.id
        

class Partition (Data):
    
    """An atomic set of nodes.
    
    Partitions can be reserved to run process groups on.
    
    Attributes:
    tag -- partition
    scheduled -- ? (default False)
    name -- canonical name
    functional -- the partition is available for reservations
    queue -- ?
    parents -- super(containing)-partitions
    children -- sub-partitions
    size -- number of nodes in the partition
    
    Properties:
    state -- "idle", "busy", or "blocked"
    """
    
    fields = Data.fields + [
        "tag", "scheduled", "name", "functional",
        "queue", "size", "parents", "children", "state",
    ]
    
    def __init__ (self, spec):
        """Initialize a new partition."""
        Data.__init__(self, spec)
        spec = spec.copy()
        self.scheduled = spec.pop("scheduled", False)
        self.name = spec.pop("name", None)
        self.functional = spec.pop("functional", False)
        self.queue = spec.pop("queue", "default")
        self.size = spec.pop("size", None)
        # these hold Partition objects
        self._parents = sets.Set()
        self._children = sets.Set()
        self.state = spec.pop("state", "idle")
        self.tag = spec.get("tag", "partition")
        self.node_cards = spec.get("node_cards", [])
        # this holds partition names
        self._wiring_conflicts = sets.Set()

        self._update_node_cards()

    def _update_node_cards(self):
        if self.state == "busy":
            for nc in self.node_cards:
                nc.used_by = self.name
    
    def _get_parents (self):
        return [parent.name for parent in self._parents]
    
    parents = property(_get_parents)
    
    def _get_children (self):
        return [child.name for child in self._children]
    
    children = property(_get_children)
    
    def __str__ (self):
        return self.name
    
    def __repr__ (self):
        return "<%s name=%r>" % (self.__class__.__name__, self.name)


class PartitionDict (DataDict):
    
    """Default container for partitions.
    
    Keyed by partition name.
    """
    
    item_cls = Partition
    key = "name"


class ProcessGroup (Data):
    required_fields = ['user', 'executable', 'args', 'location', 'size', 'cwd']
    fields = Data.fields + [
        "id", "user", "size", "cwd", "executable", "env", "args", "location",
        "head_pid", "stdin", "stdout", "stderr", "exit_status",
        "mode", "kerneloptions", "true_mpi_args",
    ]
    
    _configfields = ['mmcs_server_ip', 'db2_instance', 'bridge_config', 'mpirun', 'db2_properties', 'db2_connect']
    _config = ConfigParser.ConfigParser()
    if '-C' in sys.argv:
        _config.read(sys.argv[sys.argv.index('-C') + 1])
    else:
        _config.read(Cobalt.CONFIG_FILES)
    if not _config._sections.has_key('bgpm'):
        print '''"bgpm" section missing from cobalt config file'''
        sys.exit(1)
    config = _config._sections['bgpm']
    mfields = [field for field in _configfields if not config.has_key(field)]
    if mfields:
        print "Missing option(s) in cobalt config file: %s" % (" ".join(mfields))
        sys.exit(1)

    def __init__(self, spec):
        Data.__init__(self, spec)
        self.id = spec.get("id")
        self.head_pid = None
        self.stdin = spec.get('stdin')
        self.stdout = spec.get('stdout')
        self.stderr = spec.get('stderr')
        self.exit_status = None
        self.location = spec.get('location', [])
        self.user = spec.get('user', "")
        self.executable = spec.get('executable')
        self.cwd = spec.get('cwd')
        self.size = str(spec.get('size'))
        self.mode = spec.get('mode', 'co')
        self.args = " ".join(spec.get('args', []))
        self.kerneloptions = spec.get('kerneloptions')
        self.env = spec.get('env', {})
        self.true_mpi_args = spec.get('true_mpi_args')
        
        self.start()
    
    def _start (self):
        stdin = open(self.stdin or "/dev/null", 'r')
        os.dup2(stdin.fileno(), sys.__stdin__.fileno())
        try:
            stdout = open(self.stdout or tempfile.mktemp(), 'a')
            os.chmod(stdout, 0600)
            os.dup2(stdout.fileno(), sys.__stdout__.fileno())
        except (IOError, OSError), e:
            logger.error("process group %s: error opening stdout file %s: %s (stdout will be lost)" % (self.id, stdout, e))
        try:
            stderr = open(self.stderr or tempfile.mktemp(), 'a')
            os.chmod(stderr, 0600)
            os.dup2(stderr.fileno(), sys.__stderr__.fileno())
        except (IOError, OSError), e:
            logger.error("process group %s: error opening stderr file %s: %s (stderr will be lost)" % (self.id, stderr, e))
        
        try:
            partition = self.location[0]
        except IndexError:
            raise ProcessGroupCreationError("no location")
        
        #check for valid user/group
        try:
            userid, groupid = pwd.getpwnam(self.user)[2:4]
        except KeyError:
            raise ProcessGroupCreationError("error getting uid/gid")
        inputfile = self.inputfile
        kerneloptions = self.kerneloptions
        # strip out BGLMPI_MAPPING until mpirun bug is fixed 
        mapfile = ''
        outerenv = ("BGLMPI_MAPPING", )
        if self.env.has_key('BGLMPI_MAPPING'):
            mapfile = self.env['BGLMPI_MAPPING']
        envs = " ".join(["%s=%s" % envdata for envdata in self.env.iteritems() if not envdata[0] in outerenv])
        atexit._atexit = []

        try:
            os.setgid(groupid)
            os.setuid(userid)
        except OSError:
            logger.error("failed to change userid/groupid for process group %s" % (self.id))
            os._exit(1)
        
        #os.system("%s > /dev/null 2>&1" % (self.config['db2_connect']))
        os.environ["DB_PROPERTY"] = self.config['db2_properties']
        os.environ["BRIDGE_CONFIG_FILE"] = self.config['bridge_config']
        os.environ["MMCS_SERVER_IP"] = self.config['mmcs_server_ip']
        os.environ["DB2INSTANCE"] = self.config['db2_instance']
        os.environ["LD_LIBRARY_PATH"] = "/u/bgdb2cli/sqllib/lib"
        cmd = (self.config['mpirun'], os.path.basename(self.config['mpirun']),
               '-np', self.size, '-partition', partition,
               '-mode', self.mode, '-cwd', self.cwd, '-exe', self.executable)
        if self.args:
            cmd = cmd + ('-args', self.args)
        if envs:
            cmd = cmd + ('-env',  envs)
        if kerneloptions:
            cmd = cmd + ('-kernel_options', kerneloptions)
        if mapfile:
            cmd = cmd + ('-mapfile', mapfile)
        
        # If this mpirun command originated from a user script, its arguments
        # have been passed along in a special attribute.  These arguments have
        # already been modified to include the partition that cobalt has selected
        # for the job, and can just replace the arguments built above.
        if self.true_mpi_args:
            cmd = (self.config.get('bgpm', 'mpirun'), os.path.basename(self.config.get('bgpm', 'mpirun'))) + tuple(self.true_mpi_args)
        
        os.execl(*cmd)
    
    def start (self):
        
        """Start the process group.
        
        Fork a daemonized process.
        """

        # make pipe for daemon mpirun to talk to bgsystem
        newpipe_r, newpipe_w = os.pipe()

        pid1 = os.fork()
        if not pid1:
            try:
                os.close(newpipe_r)
                os.setsid()
                pid2 = os.fork()
                if not pid2:
                    self._start()
                else:
                    newpipe_w = os.fdopen(newpipe_w, 'w')
                    newpipe_w.write(str(pid2))
                    newpipe_w.close()
                    os._exit(0)
            except Exception, e:
                print >> sys.stderr, "when trying to fork for mpirun:", e
                traceback.print_exc(file=sys.stderr)
                os._exit(1)

        else:
            #parent process reads daemon child's pid through pipe
            os.close(newpipe_w)
            newpipe_r = os.fdopen(newpipe_r, 'r')
            self.head_pid = newpipe_r.read()
            newpipe_r.close()
            rc = os.waitpid(pid1, 0)
            logger.info('intermediate process %d exited with status %d' % rc)


class ProcessGroupDict (DataDict):
    item_cls = ProcessGroup
    key = "id"
    
    def __init__(self):
        self.id_gen = IncrID()
 
    def q_add (self, specs, callback=None, cargs={}):
        for spec in specs:
            if spec.get("id", "*") != "*":
                raise DataCreationError("cannot specify an id")
            spec['id'] = self.id_gen.next()
        return DataDict.q_add(self, specs)


class BGSystem (Component):
    
    """Blue Gene system component.
    
    Methods:
    configure -- load partitions from the bridge API
    get_partitions -- retrieve partitions managed by cobalt (exposed, query)
    add_process_groups -- add (start) an mpirun process on the system (exposed, ~query)
    get_process_groups -- retrieve running mpirun processes (exposed, query)
    """
    
    name = "system"
    implementation = "bluegene"
    
    logger = logger
    
    def __init__ (self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self._partitions = PartitionDict()
        self._managed_partitions = sets.Set()
        self.process_groups = ProcessGroupDict()
        self.node_card_cache = dict()
        self._partitions_lock = thread.allocate_lock()
        self.configure()
        
        thread.start_new_thread(self.update_partition_state, tuple())
    
    def _get_partitions (self):
        return PartitionDict([
            (partition.name, partition) for partition in self._partitions.itervalues()
            if partition.name in self._managed_partitions
        ])
    
    partitions = property(_get_partitions)

    def __getstate__(self):
        return {'managed_partitions':self._managed_partitions, 'version':1}
    
    def __setstate__(self, state):
        self._managed_partitions = state['managed_partitions']
        self._partitions = PartitionDict()
        self.process_groups = ProcessGroupDict()
        self.node_card_cache = dict()
        self._partitions_lock = thread.allocate_lock()

        self.configure()
        
        thread.start_new_thread(self.update_partition_state, tuple())
    def save_me(self):
        Component.save(self, '/var/spool/cobalt/bgsystem')
    save_me = automatic(save_me)

    def configure (self):
        
        """Read partition data from the bridge."""
        
        def _get_state(bridge_partition):
            if bridge_partition.state == "RM_PARTITION_FREE":
                return "idle"
            else:
                return "busy"
    
        def _get_node_card(name):
            if not self.node_card_cache.has_key(name):
                self.node_card_cache[name] = NodeCard(name)
                
            return self.node_card_cache[name]
            
        self.logger.info("configure()")
        try:
            system_def = Cobalt.bridge.PartitionList.by_filter()
        except BridgeException:
            print "Error communicating with the bridge during initial config.  Terminating."
            sys.exit(1)

        # that 32 is not really constant -- it needs to either be read from cobalt.conf or from the bridge API
        NODES_PER_NODECARD = 32
                
        # initialize a new partition dict with all partitions
        #
        partitions = PartitionDict()
        
        tmp_list = []

        # this is going to hold partition objects from the bridge (not our own Partition)
        wiring_cache = {}
        
        for partition_def in system_def:
            node_list = []
            nc_count = len(list(partition_def.node_cards))
            if partition_def.connection == "RM_TORUS":
                if not wiring_cache.has_key(nc_count):
                    wiring_cache[nc_count] = []
                wiring_cache[nc_count].append(partition_def)

            if partition_def.small:
                bp_name = partition_def.base_partitions[0].id
                for nc in partition_def._node_cards:
                    node_list.append(_get_node_card(bp_name + "-" + nc.id))
            else:
                try:
                    for bp in partition_def.base_partitions:
                        bp_name = bp.id
                        for nc in Cobalt.bridge.NodeCardList.by_base_partition(bp):
                            node_list.append(_get_node_card(bp_name + "-" + nc.id))
                except BridgeException:
                    print "Error communicating with the bridge during initial config.  Terminating."
                    sys.exit(1)

            tmp_list.append( dict(
                name = partition_def.id,
                queue = "default",
                size = NODES_PER_NODECARD * nc_count,
                node_cards = node_list,
                state = _get_state(partition_def),
            ))
        
        partitions.q_add(tmp_list)
        
        # find the wiring deps
        start = time.time()
        for size in wiring_cache:
            for p in wiring_cache[size]:
                s1 = sets.Set( [s.id for s in p.switches] )
                for other in wiring_cache[size]:
                    if (p.id == other.id):
                        continue

                    s2 = sets.Set( [s.id for s in other.switches] )
                    
                    if s1.intersection(s2):
                        print "%s and %s have a wiring conflict" % (p.id, other.id)
                        partitions[p.id]._wiring_conflicts.add(other.id)
        
        end = time.time()
        print "took %f seconds to find wiring deps" % (end - start)
 
        # update state information
        for p in partitions.values():
            if p.state != "busy":
                for nc in p.node_cards:
                    if nc.used_by:
                        p.state = "blocked (%s)" % nc.used_by
                        break
                for dep_name in p._wiring_conflicts:
                    if partitions[dep_name].state == "busy":
                        p.state = "blocked-wiring (%s)" % dep_name
                        break
        
        # update object state
        self._partitions.clear()
        self._partitions.update(partitions)
    
    def update_partition_state(self):
        """Use the quicker bridge method that doesn't return nodecard information to update the states of the partitions"""
        
        def _get_state(bridge_partition):
            if bridge_partition.state == "RM_PARTITION_FREE":
                return "idle"
            else:
                return "busy"

        while True:
            try:
                system_def = Cobalt.bridge.PartitionList.info_by_filter()
            except BridgeException:
                self.logger.error("Error communicating with the bridge to update partition state information.")
                return
    
            # first, set all of the nodecards to not busy
            for nc in self.node_card_cache.values():
                nc.used_by = ''
                
            self._partitions_lock.acquire()

            for partition in system_def:
                if self._partitions.has_key(partition.id):
                    self._partitions[partition.id].state = _get_state(partition)
                    self._partitions[partition.id]._update_node_cards()
                
            for p in self._partitions.values():
                if p.state != "busy":
                    for nc in p.node_cards:
                        if nc.used_by:
                            p.state = "blocked (%s)" % nc.used_by
                            break
                    for dep_name in p._wiring_conflicts:
                        if self._partitions[dep_name].state == "busy":
                            p.state = "blocked-wiring (%s)" % dep_name
                            break
                        
            self._partitions_lock.release()
            
            time.sleep(10)
    
    def update_relatives(self):
        """Call this method after changing the contents of self._managed_partitions"""
        for p_name in self._managed_partitions:
            self._partitions[p_name]._parents = sets.Set()
            self._partitions[p_name]._children = sets.Set()

        for p_name in self._managed_partitions:
            p = self._partitions[p_name]
            
            # toss the wiring dependencies in with the parents
            for dep_name in p._wiring_conflicts:
                if dep_name in self._managed_partitions:
                    p._parents.add(self._partitions[dep_name])
            
            for other_name in self._managed_partitions:
                if p.name == other_name:
                    break

                other = self._partitions[other_name]
                p_set = sets.Set(p.node_cards)
                other_set = sets.Set(other.node_cards)

                # if p is a subset of other, then p is a child
                if p_set.intersection(other_set)==p_set:
                    p._parents.add(other)
                    other._children.add(p)
                # if p contains other, then p is a parent
                elif p_set.union(other_set)==p_set:
                    p._children.add(other)
                    other._parents.add(p)
    
    def add_partitions (self, specs):
        self.logger.info("add_partitions(%r)" % (specs))
        specs = [{'name':spec.get("name")} for spec in specs]
        
        self._partitions_lock.acquire()
        partitions = [
            partition for partition in self._partitions.q_get(specs)
            if partition.name not in self._managed_partitions
        ]
        self._partitions_lock.release()
        
        self._managed_partitions.update([
            partition.name for partition in partitions
        ])
        self.update_relatives()
        return partitions
    add_partition = exposed(query(add_partitions))
    
    def get_partitions (self, specs):
        """Query partitions on simulator."""
        self.logger.info("get_partitions(%r)" % (specs))
        
        self._partitions_lock.acquire()
        partitions = self.partitions.q_get(specs)
        self._partitions_lock.release()
        
        return partitions
    get_partitions = exposed(query(get_partitions))
    
    def del_partitions (self, specs):
        self.logger.info("del_partitions(%r)" % (specs))
        
        self._partitions_lock.acquire()
        partitions = [
            partition for partition in self._partitions.q_get(specs)
            if partition.name in self._managed_partitions
        ]
        self._partitions_lock.release()
        
        self._managed_partitions -= sets.Set( [partition.name for partition in partitions] )
        self.update_relatives()
        return partitions
    del_partitions = exposed(query(del_partitions))
    
    def set_partitions (self, specs, updates):
        def _set_partitions(part, newattr):
            part.update(newattr)
            
        self._partitions_lock.acquire()
        partitions = self._partitions.q_get(specs, _set_partitions, updates)
        self._partitions_lock.release()
        
        return partitions
    set_partitions = exposed(query(set_partitions))
    
    def add_process_groups (self, specs):
        
        """Create a process group.
        
        Arguments:
        spec -- dictionary hash specifying a process group to start
        """
        
        return self.process_groups.q_add(specs)
    
    add_process_groups = exposed(query(all_fields=True)(add_process_groups))
    
    def get_process_groups (self, specs):
        self.wait_process_groups(specs) # clear zombie mpiruns
        return self.process_groups.q_get(specs)
    get_process_groups = exposed(query(get_process_groups))
    
    def _get_exit_status (self):
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except OSError: # there are no child processes
                break
            if pid == 0: # there are no zombie processes
                break
            status = status >> 8
            for each in self.process_groups.itervalues():
                if each.head_pid == pid:
                    each.exit_status = status
    
    def wait_process_groups (self, specs):
        self._get_exit_status()
        process_groups = [pg for pg in self.process_groups.q_get(specs) if pg.exit_status is not None]
        for process_group in process_groups:
            id = getattr(process_group, process_groups.key)
            del self.process_groups[id]
        return process_groups
    wait_process_groups = exposed(query(wait_process_groups))
    
    def signal_process_groups (self, specs, signame="SIGINT"):
        my_process_groups = self.process_groups.q_get(specs)
        for pg in my_process_groups:
            try:
                os.kill(int(pg.head_pid), getattr(signal, signame))
            except OSError, e:
                self.logger.error("signal failure for process group %s: %s" % (pg.id, e))
        return my_process_groups
    signal_process_groups = exposed(query(signal_process_groups))

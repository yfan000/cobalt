"""hardware abstraction layer for the system on which jobs are run"""

import pwd
import atexit
import sets
import logging
import sys
import os
import operator
import signal
import tempfile
from ConfigParser import ConfigParser

import lxml
import lxml.etree

from Cobalt.Data import Data, DataDict, get_spec_fields
from Cobalt.Components.base import Component, exposed

__all__ = [
    "ProcessGroupCreationError",
    "Partition", "PartitionDict",
    "Brooklyn",
]


class ProcessGroupCreationError (Exception):
    """Not enough information is specified"""


class Partition (Data):
    fields = Data.fields.copy()
    fields.update(dict(
        tag = "partition",
        scheduled = False,
        name = None,
        functional = False,
        queue = "default",
        size = None,
        parents = None,
        children = None,
        nodes = None,
        state = None,
    ))
    
    def __init__ (self, *args, **kwargs):
        self.parents = sets.Set()
        self.children = sets.Set()
        self.nodes = sets.Set()
        Data.__init__(self, *args, **kwargs)
    
    def __str__ (self):
        return self.name
    
    def __repr__ (self):
        return "<%s name=%r>" % (self.__class__.__name__, self.name)
    
    def _get_state (self):
        busy_nodes = sets.Set([node for node in self.nodes if node.state == "busy"])
        if not busy_nodes:
            return "idle"
        busy_parents = sets.Set([partition for partition in self.parents if partition.state == "busy"])
        if not busy_parents:
            if len(busy_nodes) == len(self.nodes):
                return "busy"
        return "blocked"
    state = property(_get_state)


class Node (Data):
    
    fields = Data.fields.copy()
    fields.update(dict(
        tag = "nodecard",
        bpid = None,
        id = None,
        queue = "default",
        state = "idle",
    ))
    
    def _get_name (self):
        return "%s-%s" % (self.bpid, self.id)
    name = property(_get_name)


class PartitionDict (DataDict):
    item_cls = Partition
    key = "name"


class NodeDict (DataDict):
    item_cls = Node
    key = "name"


class Brooklyn (Component):
    
    """A BlueGene/L bridge simulator."""
    
    name = "system"
    implementation = "brooklyn"
    
    logger = logging.getLogger("Cobalt.Components.Brooklyn")

    def __init__ (self, config_file=None, *args, **kwargs):
        """Initialize a Brooklyn simulator.
        
        Arguments:
        config_file -- automatically configure using this xml file (optional)
        """
        Component.__init__(self, *args, **kwargs)
        self.partitions = PartitionDict()
        self.nodes = NodeDict()
        # fraction of simulated jobs that will
        # run over their specified wall times
        self.overtime_frac = 0.0
        # fraction of simulated jobs that will
        # fail to cleanly release their partitions
        self.failed_release_frac = 0.0
        if config_file is not None:
            self.configure(config_file)
    
    def check_pid (self, pid):
        """checks if the specified pid is still around"""
        process_list = os.popen("ps ax").readlines()
        pids = [process.split()[0] for process in process_list]
        return str(pid) in pids
    
    def configure (self, config_file):
        
        """Configure simulated partitions and nodes.
        
        Arguments:
        config_file -- xml configuration file.
        """
        
        system_doc = lxml.etree.parse(config_file)
        system_def = system_doc.getroot()
        
        # initialize a new node dict with all nodes
        nodes = NodeDict()
        nodes.q_add([
            dict(
                bpid = node_def.get("bpid"),
                id = node_def.get("id"),
            )
            for node_def in system_def.getiterator("Nodecard")
        ])
        
        # initialize a new partition dict with all partitions
        partitions = PartitionDict()
        partitions.q_add([
            dict(
                name = partition_def.get("name"),
                queue = "default",
                scheduled = True,
                functional = True,
                size = partition_def.get("size"),
            )
            for partition_def in system_def.getiterator("Partition")
        ])
        
        # parent/child and nodes
        for partition_def in system_def.getiterator("Partition"):
            partition = partitions[partition_def.get("name")]
            partition.children.update([
                partitions[child_def.get("name")]
                for child_def in partition_def.getiterator("Partition")
                if child_def.get("name") != partition.name
            ])
            for child in partition.children:
                child.parents.add(partition)
            partition.nodes.update([
                nodes["%s-%s" % (node_def.get("bpid"), node_def.get("id"))]
                for node_def in partition_def.getiterator("Nodecard")
            ])
        
        # update object state
        self.nodes.clear()
        self.nodes.update(nodes)
        self.partitions.clear()
        self.partitions.update(partitions)
    
    def get_possible_nodegroups (self, group_size):
        """returns list of possible groups of nodes of size group_size"""
        nodes = list(self.nodes)
        nodes.sort()
        if group_size > len(nodes):
            self.logger.error("get_possible_nodegroups(%r) [impossible]")
            return []
        possible_groups = []
        for x in range(0, len(nodes), group_size):
            possible_groups.append(nodes[x:x+group_size])
        return possible_groups
    
    def get_db2_state (self):
        """Return db2-like list of tuples describing state."""
        busy_partitions = self.partitions.q_get([{'state':"busy"}])
        return [
            (partition.name, partition in busy_partitions and 'I' or 'F')
            for partition in self.partitions
        ]
    get_db2_state = exposed(get_db2_state)

    def get_nodes (self, specs):
        fields = get_spec_fields(specs)
        specs = [node.to_rx(fields) for node in self.nodes.q_get(specs)]
        specs.sort(key=operator.itemgetter('bpid', 'id'))
        return specs
    get_nodes = exposed(get_nodes)
    
    def get_partitions (self, specs):
        partitions = self.partitions.q_get(specs)
        fields = get_spec_fields(specs)
        return [partition.to_rx(fields) for partition in partitions]
    get_partitions = exposed(get_partitions)
    
    def reserve_partition (self, name, size=None):
        """Reserve a partition and block all related partitions.
        
        Arguments:
        name -- name of the partition to reserve
        size -- size of the job reserving the partition (optional)
        """
        try:
            partition = self.partitions[name]
        except KeyError:
            self.logger.error("reserve_partition(%r, %r) [does not exist]" % (name, size))
            return False
        if partition in self.partitions.q_get([{'status':"busy"}]):
            self.logger.error("reserve_partition(%r, %r) [busy]" % (name, size))
            return False
        if partition in self.partitions.q_get([{'status':"blocked"}]):
            self.logger.error("reserve_partition(%r, %r) [blocked]" % (name, size))
            return False
        if size is not None and size > partition.size:
            self.logger.error("reserve_partition(%r, %r) [size mismatch]" % (name, size))
            return False
        for node in partition.nodes:
            node.state = "busy"
        self.logger.info("reserve_partition(%r, %r)" % (name, size))
        return True
    reserve_partition = exposed(reserve_partition)
    
    def release_partition (self, name):
        """Release a reserved partition.
        
        Arguments:
        name -- name of the partition to release
        """
        try:
            partition = self.partitions[name]
        except KeyError:
            self.logger.error("release_partition(%r) [already free]" % (name))
            return False
        if not partition.state == "busy":
            self.logger.info("release_partition(%r) [not busy]" % (name))
            return False
        for node in partition.nodes:
            node.state = "idle"
        self.logger.info("release_partition(%r)")
        return True
    release_partition = exposed(release_partition)
    
    def _set_stdfiles (self, jobspec):
        inputfile = jobspec.get("inputfile", None)
        if inputfile:
            os.dup2(open(inputfile, 'r').fileno(), sys.__stdin__.fileno())
        else:
            os.dup2(open("/dev/null", 'r').fileno(), sys.__stdin__.fileno())
        outlog = jobspec.get("outputfile", None)
        if not outlog:
            outlog = tempfile.mktemp()
        try:
            stdout = open(outlog, 'a')
        except IOError, e:
            self.logger.error("job %s/%s: Failed to open %s. (%s) stdout will be lost." % ((jobspec['jobid']), jobspec['user'], outlog, e))
        try:
            os.chmod(outlog, 0600)
            os.dup2(stdout.fileno(), sys.__stdout__.fileno())
        except OSError, e:
            self.logger.error("job %s/%s: Failed to chmod or dup2 %s. (%s) stderr will be lost." % (jobspec['jobid'], jobspec['user'], outlog, e))
        errlog = jobspec.get("errorfile", None)
        if not errlog:
            errlog = tempfile.mktemp()
        try:
            stderr = open(errlog, 'a')
        except IOError, e:
            self.logger.error("job %s/%s: Failed to open %s. (%s) stderr will be lost." % ((jobspec['jobid']), jobspec['user'], errlog, e))
        try:
            os.chmod(errlog, 0600)
            os.dup2(stderr.fileno(), sys.__stderr__.fileno())
        except OSError, e:
            self.logger.error("job %s/%s: Failed to chmod or dup2 %s. (%s) stderr will be lost." % (jobspec['jobid'], jobspec['user'], errlog, e))
    
    def _set_owner (self, jobspec):
        user_name = jobspec.get("user", None)
        if not user_name:
            raise ProcessGroupCreationError("user")
        try:
            uid, gid = pwd.getpwnam()[2:4]
        except KeyError:
            raise ProcessGroupCreationError("user")
        try:
            os.setgid(gid)
        except OSError, e:
            self.logger.error("unable to change gid for process group %s (%s)" % (jobspec['pgid'], e))
            sys.exit(0)
        try:
            os.setuid(uid)
        except OSError, e:
            self.logger.error("unable to change uid for process group %s (%s)" % (jobspec['pgid'], e))
            sys.exit(0)
    
    def _set_env (self, jobspec, config_files=["/etc/cobalt.conf"]):
        config = ConfigParser()
        config.read(config_files)
        os.environ["DB_PROPERTY"] = config.get("bgpm", "db2_properties")
        os.environ["BRIDGE_CONFIG_FILE"] = config.get("bgpm", "bridge_config")
        os.environ["MMCS_SERVER_IP"] = config.get("bgpm", "mmcs_server_ip")
        os.environ["DB2INSTANCE"] = config.get("bgpm", "db2_instance")
        os.environ["LD_LIBRARY_PATH"] = "/u/bgdb2cli/sqllib/lib"
        os.environ["COBALT_JOBID"] = jobspec['jobid']
        # special stuff just for the simulator
        # to make some jobs fail to behave nicely
        os.environ["OVERTIME_FRAC"] = str(self.overtime_frac)
        os.environ["FAILED_RELEASE_FRAC"] = str(self.failed_release_frac)
    
    def _build_command (self, jobspec, config_files=["/etc/cobalt.conf"]):
        config = ConfigParser()
        config.read(config_files)
        
        cmd = [
            config.get("bgpm", "mpirun"),
            os.path.basename(config.get("bgpm", "mpirun")),
        ]
        
        if "true_mpi_args" in jobspec:
            # arguments have been passed along in a special attribute.  These arguments have
            # already been modified to include the partition that cobalt has selected
            # for the job, and can just replace the arguments built above.
            cmd.extend(jobspec['true_mpi_args'])
        
        else:
            
            cmd.extend([
                "-np", str(jobspec['size']),
                "-mode", jobspec.get("mode", "co"),
                "-cwd", jobspec['cwd'],
                "-exe", jobspec['executable'],
            ])
            
            try:
                partition = jobspec["location"][0]
            except (KeyError, IndexError):
                raise ProcessGroupCreationError("location")
            cmd.extend(["-partition", partition])
            
            kerneloptions = jobspec.get("kerneloptions", None)
            if kerneloptions:
                cmd.extend(['-kernel_options', kerneloptions])
            
            args = jobspec.get('args', [])
            if args:
                cmd.extend(["-args", " ".join(args)])
            
            envs = jobspec.get("envs", None)
            if envs:
                env_kvstring = " ".join(["%s=%s" % (key, value) for key, value in envs.iteritems()])
                cmd.extend(["-env",  env_kvstring])
            
            if "BGLMPI_MAPPING" in jobspec.get("env", {}):
                # strip out BGLMPI_MAPPING until mpirun bug is fixed
                mapfile = jobspec['env']['BGLMPI_MAPPING']
                del jobspec['env']['BGLMPI_MAPPING']
                cmd.extend(["-mapfile", mapfile])
        return cmd
    
    def start_job (self, jobspec):
        
        """Start a simulated job as a local process.
        
        Arguments:
        jobspec -- dictionary hash specifying a job to start
        """
        
        pid_pipe_r, pid_pipe_w = os.pipe()
        child_pid = os.fork()
        
        # parent process
        if child_pid != 0:
            # read daemon child's pid through pipe
            os.close(pid_pipe_w)
            pid_pipe_r = os.fdopen(pid_pipe_r, 'r')
            daemon_pid = pid_pipe_r.read()
            pid_pipe_r.close()
            # wait for the intermediate process to finish
            child_pid, child_exit_status = os.waitpid(child_pid, 0)
            self.logger.info("daemonizing process %s exited with status %s" % (child_pid, child_exit_status))
            self.logger.info("started daemon process %s" % (daemon_pid))
            jobspec['pid'] = daemon_pid
            return jobspec
        
        # intermediate (daemonizing) process
        os.close(pid_pipe_r)
        os.setsid()
        daemon_pid = os.fork()
        if daemon_pid != 0:
            pid_pipe_w = os.fdopen(pid_pipe_w, 'w')
            pid_pipe_w.write(str(daemon_pid))
            pid_pipe_w.close()
            os._exit(0)
        
        # daemon process
        os.close(pid_pipe_w)
        self._set_stdfiles(jobspec)
        self._set_owner(jobspec)
        self._set_env(jobspec)
        cmd = self._build_command(jobspec)
        try:
            os.execv(cmd[0], cmd[1:])
        except Exception, e:
            print "when trying to exec mpirun: %s", e
            sys.exit(1)
        sys.exit()
    start_job = exposed(start_job)
    
    def get_jobs (self, specs):
        """returns those jobs that are running"""
        self.logger.info("query_jobs(%r)" % (specs))
        return [spec for spec in specs if self.check_pid(spec['pid'])]
    get_jobs = exposed(get_jobs)
    
    def kill_job (self, spec):
        """kill a job"""
        try:
            os.kill(int(spec['pid']), signal.SIGINT)
        except OSError, e:
            self.logger.error("Signal failure for pid %s (%s)" % (pid, e))
    kill_job = exposed(kill_job)

    def set_overtime_frac (self, val):
        self.overtime_frac = float(val)
    set_overtime_frac = exposed(set_overtime_frac)
        
    def set_failed_release_frac (self, val):
        self.failed_release_frac = float(val)
    set_failed_release_frac = exposed(set_failed_release_frac)

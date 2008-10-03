"""Hardware abstraction layer for the system on which process groups are run.

Classes:
ProcessGroup -- a group of processes started with mpirun
BGSystem -- Blue Gene system component
"""

import atexit
import pwd
import sets
import logging
import sys
import os
import signal
import tempfile
import time
import thread
import ConfigParser
import tempfile
try:
    set = set
except NameError:
    from sets import Set as set

import Cobalt
import Cobalt.Data
from Cobalt.Components import cluster_base_system
from Cobalt.Components.base import Component, exposed, automatic, query
from Cobalt.Exceptions import ProcessGroupCreationError
from Cobalt.Components.cluster_base_system import ProcessGroupDict, ClusterBaseSystem


__all__ = [
    "ProcessGroup",
    "Simulator",
]

logger = logging.getLogger(__name__)

class ProcessGroup (cluster_base_system.ProcessGroup):
    
    def __init__(self, spec):
        cluster_base_system.ProcessGroup.__init__(self, spec)
        self.nodefile = ""
        self.start()
    
    def _mpirun (self):
        #check for valid user/group
        try:
            tmp_data = pwd.getpwnam(self.user)
	    userid = tmp_data.pw_uid
	    groupid = tmp_data.pw_gid
	    homedir = tmp_data.pw_dir
        except KeyError:
            raise ProcessGroupCreationError("error getting uid/gid")
        
        try:
            os.setgid(groupid)
            os.setuid(userid)
        except OSError:
            logger.error("failed to change userid/groupid for process group %s" % (self.id))
            os._exit(1)

        try:
            os.umask(self.umask)
        except:
            logger.error("Failed to set umask to %s" % self.umask)

        self.nodefile = tempfile.mktemp(prefix=".cobalt", dir=homedir)
        fd = open(self.nodefile, "w")
	for host in self.location:
	    fd.write(host + "\n")
	fd.close()

        stdin = open(self.stdin or "/dev/null", 'r')
        os.dup2(stdin.fileno(), sys.__stdin__.fileno())
        try:
            stdout = open(self.stdout or tempfile.mktemp(), 'a')
            os.dup2(stdout.fileno(), sys.__stdout__.fileno())
        except (IOError, OSError), e:
            logger.error("process group %s: error opening stdout file %s: %s (stdout will be lost)" % (self.id, self.stdout, e))
        try:
            stderr = open(self.stderr or tempfile.mktemp(), 'a')
            os.dup2(stderr.fileno(), sys.__stderr__.fileno())
        except (IOError, OSError), e:
            logger.error("process group %s: error opening stderr file %s: %s (stderr will be lost)" % (self.id, self.stderr, e))

        rank0 = self.location[0].split(":")[0]
        env_setup = "env COBALT_NODEFILE=%s COBALT_JOBID=%s " % (self.nodefile, self.env["COBALT_JOBID"])
        cmd = ("/usr/bin/ssh", "/usr/bin/ssh", rank0, env_setup + self.executable)
        
        # If this mpirun command originated from a user script, its arguments
        # have been passed along in a special attribute.  These arguments have
        # already been modified to include the partition that cobalt has selected
        # for the job, and can just replace the arguments built above.
        if self.true_mpi_args:
            cmd = (self.config['mpirun'], os.path.basename(self.config['mpirun'])) + tuple(self.true_mpi_args)
    
        try:
            cobalt_log_file = open(self.cobalt_log_file or "/dev/null", "a")
            print >> cobalt_log_file, "%s\n" % " ".join(cmd[1:])
            cobalt_log_file.close()
        except:
            logger.error("Job %s/%s:  unable to open cobaltlog file %s" % \
                         (self.id, self.user, self.cobalt_log_file))

        os.execl(*cmd)
    
    def start (self):
        
        """Start the process group.
        
        Fork for mpirun.
        """

        child_pid = os.fork()
        if not child_pid:
            try:
                self._mpirun()
            except:
                logger.error("unable to start mpirun", exc_info=1)
                os._exit(1)
        else:
            self.head_pid = child_pid



class ClusterSystem (ClusterBaseSystem):
    
    """cluster system component.
    
    Methods:
    configure -- load partitions from the bridge API
    add_process_groups -- add (start) an mpirun process on the system (exposed, ~query)
    get_process_groups -- retrieve mpirun processes (exposed, query)
    wait_process_groups -- get process groups that have exited, and remove them from the system (exposed, query)
    signal_process_groups -- send a signal to the head process of the specified process groups (exposed, query)
    update_partition_state -- update partition state from the bridge API (runs as a thread)
    """
    
    name = "system"
    implementation = "cluster_system"
    
    logger = logger

    
    def __init__ (self, *args, **kwargs):
        ClusterBaseSystem.__init__(self, *args, **kwargs)
        self.process_groups.item_cls = ProcessGroup
        self.config_file = kwargs.get("config_file", None)
        if self.config_file is not None:
            self.configure(self.config_file)

        
    
    def add_process_groups (self, specs):
        
        """Create a process group.
        
        Arguments:
        spec -- dictionary hash specifying a process group to start
        """
        
        return self.process_groups.q_add(specs)
    
    add_process_groups = exposed(query(add_process_groups))
    
    def get_process_groups (self, specs):
        self._get_exit_status()
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
                    self.logger.info("pg %i exited with status %i" % (each.id, status))
    _get_exit_status = automatic(_get_exit_status)
    
    def wait_process_groups (self, specs):
        self._get_exit_status()
        process_groups = [pg for pg in self.process_groups.q_get(specs) if pg.exit_status is not None]
        for process_group in process_groups:
            for host in self.process_groups[process_group.id].location:
                self.running_nodes.remove(host)
            del self.process_groups[process_group.id]
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


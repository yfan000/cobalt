import copy
import ConfigParser
import logging
import os
import sys
import subprocess
import re
import Cobalt.Components.pg_forker
PGPreexec = Cobalt.Components.pg_forker.PGPreexec
PGForker = Cobalt.Components.pg_forker.PGForker
import Cobalt.Util
convert_argv_to_quoted_command_string = Cobalt.Util.convert_argv_to_quoted_command_string

_logger = logging.getLogger(__name__)

rpn_re  = re.compile(r'c(?P<pos>[0-9]*)') 

class BGRunjobPreexec(PGPreexec):
    def __init__(self, child, cmd_str, env):
        PGPreexec.__init__(self, child, cmd_str, env)

    def do_first(self):
        PGPreexec.do_first(self)

    def do_last(self):
        PGPreexec.do_last(self)


class BGRunjobForker (PGForker):
    
    """Component for starting mpirun jobs on the Blue Gene"""
    
    name = "bg_runjob_forker"
    implementation = "bg_runjob_forker"

    _configfields = ['mpirun']
    _config = ConfigParser.ConfigParser()
    _config.read(Cobalt.CONFIG_FILES)
    if not _config._sections.has_key('bgpm'):
        print '''"bgpm" section missing from cobalt config file'''
        sys.exit(1)
    config = _config._sections['bgpm']
    mfields = [field for field in _configfields if not config.has_key(field)]
    if mfields:
        print "Missing option(s) in cobalt config file: %s" % (" ".join(mfields))
        sys.exit(1)

    def __init__ (self, *args, **kwargs):
        """Initialize a new BG mpirun forker.
        
        All arguments are passed to the base forker constructor.
        """
        PGForker.__init__(self, *args, **kwargs)

    def __getstate__(self):
        return PGForker.__getstate__(self)

    def __setstate__(self, state):
        PGForker.__setstate__(self, state)

    def _fork(self, child, data):
        PGForker._fork(self, child, data)
        pg = child.pg

        postfork_env = copy.deepcopy(os.environ)

        # export subset of MPIRUN_* variables to mpirun's environment
        # we explicitly state the ones we want since some are "dangerous"
        #I don't think we have these anymore. FIXME
        exportenv = [ 'MPIRUN_CONNECTION', 'MPIRUN_KERNEL_OPTIONS',
                      'MPIRUN_MAPFILE', 'MPIRUN_START_GDBSERVER',
                      'MPIRUN_LABEL', 'MPIRUN_NW', 'MPIRUN_VERBOSE',
                      'MPIRUN_ENABLE_TTY_REPORTING', 'MPIRUN_STRACE']

        app_envs = []
        set_label = False
        set_verbose = None
        
        for key, value in pg.env.iteritems():
            if key in exportenv:
    	        if key == 'MPIRUN_LABEL':
    	            set_label = True
    	        elif key == 'MPIRUN_VERBOSE':
    	            set_verbose = int(value)
                    postfork_env[key] = value
            else:
                app_envs.append((key, value))
         
        # add the cobalt env vars last so as overwrite any value provided by the user
        self._add_cobalt_env_vars(child, postfork_env)

        #determine ranks from mode:
        
        cmd = [self.config['runjob'],
              #'-host', self.config['mmcs_server_ip'],
              #get proccount from pg
               '--np', str(int(pg.size) * int(rpn_re.match(pg.mode).groups()[0])),
               '--block', pg.partition, #corner and shape derived from this.
               '--ranks-per-node', rpn_re.match(pg.mode).groups()[0], #default 1.  valid values are 2^n for n <= 6.
               '--cwd', pg.cwd]
        #if pg.args:
        #    cmd.extend(['--args', " ".join(pg.args)]
        if len(app_envs) > 0:
            for e in app_envs:
                cmd.extend(['--envs', ("%s=%s" % e)])
            #cmd.extend(['--envs', " ".join(["%s=%s" % x for x in app_envs])])
        #if pg.kerneloptions: FIXME: No kernel support yet
        #    cmd.extend(['-kernel_options', pg.kerneloptions])
        
        if set_label:
            cmd.extend(['--label', 'long'])
        if set_verbose != None:
            cmd.extend(['--verbose', str(set_verbose)])
        else:
            cmd.extend(['--verbose', '4'])

        #last append the binary and it's args:
        cmd.extend([':',pg.executable])
        if pg.args:
            cmd.extend(pg.args)

        try:
            preexec_fn = BGRunjobPreexec(child, convert_argv_to_quoted_command_string(cmd), postfork_env)
        except:
            _logger.error("%s: instantiation of preexec class failed; aborting execution")
            raise

        try:
            child.proc = subprocess.Popen(cmd, preexec_fn=preexec_fn, env=postfork_env)
            child.pid = child.proc.pid
            _logger.info("%s: forked with pid %s", child.label, child.pid)
        except OSError as e:
            _logger.error("%s: failed to execute with a code of %s: %s", child.label, e.errno, e)
        except ValueError:
            _logger.error("%s: failed to run due to bad arguments.", child.label)
        except Exception as e:
            _logger.error("%s: failed due to an unexpected exception: %s", child.label, e)
            _logger.debug("%s: Parent Traceback:", child.label, exc_info=True)
            if e.__dict__.has_key('child_traceback'):
                _logger.debug("%s: Child Traceback:\n %s", child.label, e.child_traceback)
            #It may be valuable to get the child traceback for debugging.
            raise
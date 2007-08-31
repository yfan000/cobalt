#!/usr/bin/env python

'''Cobalt fake mpirun'''
__revision__ = ''
__version__ = '$Version$'

import getopt, os, pwd, sys, time, xmlrpclib, logging
import Cobalt.Logging, Cobalt.Proxy, Cobalt.Util

usehelp = "Usage:\nzzzz [--version] [-f] <jobid> <jobid>"

if __name__ == '__main__':
    if '--version' in sys.argv:
        print "zzzz %s" % __revision__
        print "cobalt %s" % __version__
        raise SystemExit, 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f')
    except getopt.GetoptError, gerr:
        print gerr
        print usehelp
        raise SystemExit, 1
    if len(args) < 1:
        execname = "/bin/ls"
    else:
        execname = args[0]
    level = 30
    if '-d' in sys.argv:
        level = 10
        
    Cobalt.Logging.setup_logging('fake-mpirun', to_syslog=False, level=level)
    logger = logging.getLogger('fake-mpirun')
    
    user = pwd.getpwuid(os.getuid())[0]
    jobspec = {'jobid':"oo", 'command':execname, 'user':user, 'nodes':'100', 'procs':'100', 'walltime':'1', 'mode':'vn', 'args':'', 'outputdir':'/tmp', 'path':''}
    try:
        cqm = Cobalt.Proxy.queue_manager()

        # try adding job to queue_manager
        pgid = cqm.ScriptMPI(jobspec)
        print "i see pgid of : ", pgid

    except Cobalt.Proxy.CobaltComponentError:
        logger.error("Can't connect to the queue manager")
        raise SystemExit, 1
    except xmlrpclib.Fault, flt:
        if flt.faultCode == 31:
            logger.error("System draining. Try again later")
            raise SystemExit, 1
        elif flt.faultCode == 30:
            logger.error("Job submission failed because: \n%s\nCheck 'cqstat -q' and the cqstat manpage for more details." % flt.faultString)
            raise SystemExit, 1
        elif flt.faultCode == 1:
            logger.error("Job submission failed due to queue-manager failure")
            raise SystemExit, 1
        else:
            logger.error("Job submission failed")
            logger.error(flt)
            raise SystemExit, 1
#     except:
#         logger.error("Error submitting job")
#         raise SystemExit, 1

    result = "yay!"
    #result = Cobalt.Proxy.process_manager().WaitProcessGroup([{'tag':'process-group', 'pgid':pgid, 'exit-status':'*'}])

    print "all done with result : %s" % result
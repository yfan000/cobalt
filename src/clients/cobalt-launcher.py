#!/usr/bin/env python

import sys
import os
import optparse


if __name__ == '__main__':
    p = optparse.OptionParser()
    
    p.add_option("--cwd", action="store", dest="cwd", help="current working directory to use")
    p.add_option("--jobid", action="store", dest="jobid", help="jobid")
    p.add_option("--nf", action="store", dest="nodefile", help="nodefile")
    p.add_option("--exe", action="store", dest="executable", help="executable path")

    if len(sys.argv) == 1:
        p.print_help()
        sys.exit(1)
        
    opt, args = p.parse_args()

    env = {}
    env['COBALT_JOBID'] = opt.jobid
    env['COBALT_NODEFILE'] = opt.nodefile
    env['PATH'] = os.environ['PATH']

    os.chdir(opt.cwd)
    os.execvpe(opt.executable, (opt.executable, ), env)

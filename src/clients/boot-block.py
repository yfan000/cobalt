#!/usr/bin/env python
"""
Prompt cobalt to boot a block on behalf of a user.

Usage: %prog [options]
version: "%prog " + __revision__ + , Cobalt  + __version__

OPTIONS DEFINITIONS:

'-d','--debug',dest='debug',help='turn on communication debugging',callback=cb_debug
'--block', dest='block', type='string', help='Name of block to boot.'
'--reboot', action='store_true', dest='reboot', help='If the block is already booted, free the block and reboot.'
'--free', action='store_true', dest='free', help='Free the block, if booted.  May not be combined with reboot'
'--jobid', dest='jobid', type='int', help='Specify a cobalt jobid for this boot.',callback=cb_gtzero

"""
import logging
import sys
import os
from Cobalt import client_utils
from Cobalt.client_utils import cb_debug, cb_gtzero

from Cobalt.arg_parser import ArgParse

__revision__= 'TBD'
__version__ = '$Version$'

AUTH_FAIL       = 2
BAD_OPTION_FAIL = 3
GENERAL_FAIL    = 1
SUCCESS         = 0

def main():
    """
    get-bootable-blocks main
    """
    # setup logging for client. The clients should call this before doing anything else.
    client_utils.setup_logging(logging.INFO)

    # read the cobalt config files
    client_utils.read_config()

    # list of callback with its arguments
    callbacks = [
        # <cb function>     <cb args>
        [ cb_debug        , () ],
        [ cb_gtzero       , () ] ]

    # Get the version information
    opt_def =  __doc__.replace('__revision__',__revision__)
    opt_def =  opt_def.replace('__version__',__version__)

    parser = ArgParse(opt_def,callbacks)

    user = client_utils.getuid()

    parser.parse_it() # parse the command line
    opts   = parser.options

    if not parser.no_args():
        client_utils.logger.info('No arguments needed')

    if opts.free and opts.reboot:
        client_utils.logger.error("ERROR: --free may not be specified with --reboot.")
        sys.exit(BAD_OPTION_FAIL)

    block = opts.block
    if block == None:
        try:
            block = os.environ['COBALT_PARTNAME']
        except KeyError:
            pass
        try:
            block = os.environ['COBALT_BLOCKNAME']
        except KeyError:
            pass
        if block == None:
            client_utils.logger.error("ERROR: block not specified as option or in environment.")
            sys.exit(BAD_OPTION_FAIL)

    jobid = opts.jobid
    if jobid == None:
        try:
            jobid = os.environ['COBALT_JOBID']
        except KeyError:
            client_utils.logger.error("ERROR: Cobalt jobid not specified as option or in environment.")
            sys.exit(BAD_OPTION_FAIL)

    # Get the system component
    system = client_utils.client_data.system_manager(False)

    if opts.reboot or opts.free:
        #Start the free on the block
        #poke cobalt to kill all jobs on the resource as well.
        success = system.initiate_proxy_free(block, user, jobid)
        client_utils.logger.info("Block free on %s initiated." % (block,))
        if not success:
            client_utils.logger.error("Free request for block %s failed authorization." % (block, ))
            sys.exit(AUTH_FAIL)
        while (True):
            #wait for free.  If the user still has jobs running, this won't complete.
            #the proxy free should take care of this, though.
            if system.get_block_bgsched_status(block) == 'Free':
                client_utils.logger.info("Block %s successfully freed." % (block,))
                break

    if not opts.free:
        success = system.initiate_proxy_boot(block, user, jobid)
        if not success:
            client_utils.logger.error("Boot request for block %s failed authorization." % (block, ))
            sys.exit(AUTH_FAIL)
        #give the system component a moment to initiate the boot
        client_utils.sleep(3)
        #wait for block to boot
        failed = False
        found = False
        while True:
            boot_id, status, status_strings = system.get_boot_statuses_and_strings(block)
            if not found:
                if boot_id != None:
                    found = True
            else:
                if status_strings != [] and status_strings != None:
                    print "\n".join(status_strings)
                if status in ['complete', 'failed']:
                    system.reap_boot(block)
                    if status == 'failed':
                        failed = True
                    break
            client_utils.sleep(1)
        if failed:
            client_utils.logger.error("Boot for locaiton %s failed."% (block,))
        else:
            client_utils.logger.info("Boot for locaiton %s complete."% (block,))

if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        raise
    except:
        client_utils.logger.fatal("*** FATAL EXCEPTION: %s ***",str(sys.exc_info()))
        raise

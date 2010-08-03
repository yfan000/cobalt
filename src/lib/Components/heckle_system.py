#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Heckle System Object Component
Represents, holds, operates upon, and manipulates the 
resources of the machines themselves.
"""
from types import ListType, StringType
import logging

from Cobalt.DataTypes.heckle_temp_ProcessGroup import ProcessGroup
from Cobalt.DataTypes.heckle_temp_ProcessGroup import ProcessGroupDict
from Cobalt.DataTypes.Resource import ResourceDict
from Cobalt.Components.base import Component, automatic, exposed, query
from Cobalt.Exceptions import JobValidationError

from Cobalt.Components.heckle_processgroup import HeckleProcessGroup
from Cobalt.Components.heckle_processgroup import HeckleProcessGroupDict


#from Cobalt.Components.heckle_lib import *
try:
    from Cobalt.Components.heckle_lib2 import HeckleConnector
except:
    from heckle_lib2 import HeckleConnector



__all__ = ["HeckleSystem"]


LOGGER = logging.getLogger(__name__)


class HeckleSystem(Component):
    """
    Cobalt System component for handling / interacting with Heckle resource manager
    
    External Methods:
        add_process_groups -- allocates nodes
        get_process_groups -- get process groups based on specs
        signal_process_groups -- signal a process group
        wait_process_groups -- removed process groups based on specs
        
    Internal Methods:
        __init__:
        _start_pg:
        _check_builds_done:
        _wait:
        _release_resources:
        get_resources:
        
    Queue Manager Methods:
        validate_job:
        verify_locations:
        find_job_locations:
        find_queue_equivalence_classes:
    """
        
    name = "system"
    implementation = "HeckleBreadboard"
    queue_assignments = {}

    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self.process_groups = ProcessGroupDict()
        self.process_groups.item_cls = HeckleProcessGroup
        self.queue_assignments["default"] = self.get_resources()
        #print "\n\n\n\n"
        #print "Queue assignments are: %s" % self.queue_assignments
    def __repr__(self):
        """
        printout representation of the class
        """
        indict = self.__dict__
        printstr = ""
        printstr += "Heckle System Object: Values"
        for element in indict:
            printstr += str(element) + "::"
            if indict[element] == None:
                printstr += "None, "
            else:
                printstr += str(indict[element]) + ", "
        printstr += "   Process Groups:"
        for element in self.process_groups:
            printstr += str(element) + "::" + \
            str(self.process_groups[element]) + ", "
        return printstr
    #####################
    # Main set of methods
    #####################
    def add_process_groups(self, specs):
        """
        Allocate nodes and add the list of those allocated to the PGDict
        specs is a list of dictionaries
        Each dictionary contains the specifications for all the nodes in the process group
        """
        LOGGER.debug( 
        "System:add_process_groups: Specs are %s" % specs )
        hiccup = HeckleConnector()
        try:     #  Checking for Fakebuild
            specs[0]['fakebuild'] = specs[0]['env']['fakebuild']
            del specs[0]['env']['fakebuild']
        except:
            pass
        print "System: add_process_groups:   %s" % specs
        #try:
        reservation = hiccup.make_reservation( **(specs[0]) )
        heckle_res_id = reservation.id
        uid = specs[0]['user']
        LOGGER.debug( "System:heckle_res_id = %i" % heckle_res_id )
        specs[0]['heckle_res_id'] = heckle_res_id
        return self.process_groups.q_add(specs, lambda x, _:\
        self._start_pg(x, heckle_res_id = heckle_res_id, uid=uid))
    add_process_groups = exposed(query(add_process_groups))
    
    
    def get_process_groups(self, specs):
        """get a list of existing allocations"""
        #LOGGER.debug( "System:get_process_groups" )
        self._wait()
        return self.process_groups.q_get(specs)
    get_process_groups = exposed(query(get_process_groups))
    
    
    def signal_process_groups(self, specs, sig):
        """Free the specified process group (set of allocated nodes)"""
        LOGGER.debug( 
        "System:signal_process_groups: Specs are %s, sig is %s"\
        % (specs, sig) )
        return self.process_groups.q_get(specs, lambda x, y:x.signal(y), sig)
    signal_process_groups = exposed(query(signal_process_groups))
    
    
    def wait_process_groups(self, specs):
        """Remove terminated process groups"""
        LOGGER.debug( "System:wait_process_groups; specs are %s" % specs )
        return self.process_groups.q_del(specs, lambda x, \
        _:self._release_resources(x))
    wait_process_groups = exposed(query(wait_process_groups))
    
    
    #########################################
    # Methods for dealing with Process Groups
    #########################################
    
    
    def _start_pg(self, pgp, heckle_res_id, uid):
        """
        Populates the process group with its resources
            gets node information for nodes in process group
            Updates those attributes
            Places nodes in the pinging nodes list, to see if they're built
        """
        LOGGER.debug( "System:start_pg: PGP is %s" % pgp )
        hiccup = HeckleConnector()
        for node in pgp.get('location'):
            node_attributes = {}
            #######################
            ###  Look at this as a possible change
            #######################
            node_atts = hiccup.get_node_properties(node)
            LOGGER.debug( 
            "System:start_pg: Atts for node %s are %s" % (node, node_atts) )
            node_attributes.update( node_atts )
            node_attributes['mac'] = node_attributes['mac'].replace("-", ":")
            node_attributes['heckle_res_id'] = heckle_res_id
            pgp.resource_attributes[node] = node_attributes
            pgp.pinging_nodes.append(node)
            pgp.uid = uid
    add_process_groups = exposed(query(add_process_groups))
    
    
    def _check_builds_done(self):
        """
        Check to see if the nodes are done building
        Starts the process group if all nodes in them are done building
        """
        #LOGGER.debug( "System:Check Build Done: Waiting to Start..." )
        #sleep(20)
        exstr = "System:check_build_done:"
        retval = True
        pg_list = [x for x in self.process_groups.itervalues()\
        if (len(x.pinging_nodes) > 0)]
        hiccup = HeckleConnector()
        for pgp in pg_list:
            for nodename in pgp.pinging_nodes:
                teststr = hiccup.get_node_bootstate(nodename)
                if  teststr == "COMPLETED" or teststr == "READY":
                    LOGGER.debug( exstr +
                    "Removing node %s...%i pinging nodes left" \
                    % (nodename, len(pgp.pinging_nodes)-1) )
                    pgp.pinging_nodes.remove(nodename)
                elif teststr in ["BOOTING", "", ""]:
                    LOGGER.debug( exstr +
                    "Node %s not done yet." % nodename)
                elif teststr == "UNALLOCATED":
                    raise Exception( exstr +
        "Node 'UNALLOCATED'; Possible build error, or system timed out.")
                elif teststr == "CRITFAIL":
                    raise Exception( exstr +
                "Node says, 'CRITFAIL'.  It timed out while building.")
                #####################
                ####     Need to figure a better way to fail gracefully
                #####################
            if len(pgp.pinging_nodes) == 0:
                LOGGER.debug( 
    "System:Check Build Done: No Pinging Nodes left, Start PG Running.")
                pgp.start()
            else:
                retval = False
        return retval
    _check_builds_done = automatic(_check_builds_done)
    
    
    def _wait(self):
        """
        Calls the process group container's wait() method
        """
        #LOGGER.debug( "System:wait" )
        for pgp in self.process_groups.itervalues():
            pgp.wait()
    _wait = automatic(_wait)
    
    
    def _release_resources(self, pgp):
        """
        Releases all the Heckle nodes, unreserving them
        """
        LOGGER.debug( "System:release" )
        LOGGER.debug( "System:Locations are: %s" % pgp.location )
        hiccup = HeckleConnector()
        hiccup.free_reserved_node( uid = pgp.uid, node_list=pgp.location )
    
    
    def get_resources(self, specs={}):
        """
        Returns a list of names for all the resources (nodes) which match the given specs.
        """
        LOGGER.debug( "System:get Resources" )
        ##################################
        ###  Look at this as a future change
        ##################################
        hiccup = HeckleConnector()
        return hiccup.list_available_nodes( **specs )
    get_resources = exposed(query(get_resources))
    
    
    ##########################################################
    # Methods for interacting with scheduler and queue-manager
    ##########################################################
    
    
    def validate_job(self, spec):
        """
        Validates a job for submission
        -- will the job ever run under the current Heckle configuration?
        Steps:
            1)  Validate Kernel
            2)  Validate HW
            3)  Validate Job versus overall
        """
        LOGGER.debug( "System:Validate Job: Specs are %s" % spec )
        hiccup = HeckleConnector()
        try:
            kernel = spec['kernel']
            valid_kernel = hiccup.validkernel( kernel )
            if not valid_kernel:
                raise Exception("System:Validate Job: Bad Kernel")
        except:
            spec['kernel'] = 'default'
        try:
            valid_hw = hiccup.validhw( **spec['attrs'] )
            if not valid_hw:
                raise Exception(
                "System:Validate Job: Bad Hardware Specs: %s" % spec )
        except Exception as strec:
            raise Exception("System:Validate Job:  Validate Job: %s" % strec)
        #try:
            #valid_job = hiccup.validjob( **spec )
            #if not valid_job:
                #raise Exception(
                #"System: validate Job:  Never enough nodes")
        #except:
            #raise Exception("System: validate Job: Never enough nodes")
        return spec
    validate_job = exposed(validate_job)
    
    
    def verify_locations(self, location_list):
        """
        Makes sure a location list is valid
        location list is a list of fully qualified strings of node names
        ex:  nodename.mcs.anl.gov
        """
        LOGGER.debug("System:validate Job: Verify Locations")
        hiccup = HeckleConnector()
        heckle_set = set(hiccup.list_all_nodes())
        location_set = set(location_list)
        if heckle_set >= location_set:
            return location_list
        else:
            not_valid_list = list( location_set.difference( heckle_set ) )
            raise Exception(
    "System:VerifyLocations: Invalid location names: %s" % not_valid_list)
    verify_locations = exposed( verify_locations )
    
    
    def find_job_location(self, job_location_args, end_times):
        """
        Finds a group of not-busy nodes in which to run the job
        
        Arguments:
            job_location_args -- A list of dictionaries with info about the job
                jobid -- string identifier
                nodes -- int number of nodes
                queue -- string queue name
                required -- ??
                utility_score -- ??
                threshold -- ??
                walltime -- ??
                attrs -- dictionary of attributes to match against
            end_times -- supposed time the job will end
            
        Returns: Dictionary with list of nodes a job can run on, keyed by jobid
        """
        LOGGER.debug("System:find_job_location" )
        locations = {}
        def jobsort(job):
            """Used to sort job list by utility score"""
            return job["utility_score"]
        job_location_args.sort(key=jobsort)
        
        #Try to match jobs to nodes which can run them
        hiccup = HeckleConnector()
        for job in job_location_args:
            if "attrs" not in job or job["attrs"] is None:
                job["attrs"] = {}
            print "Job is %s" % job
            #############################
            ###  Look at this as point of change
            ###  Think:  For node in unreserved nodes
            ###            Choose node from list
            ###            Remove node from unreserved nodes
            #############################
            resources = hiccup.find_job_location(**job)  #get matching nodes
            if not resources:
                continue
            node_list = []
            # Build a list of appropriate nodes
            for node in resources:
                node_list.append(node)
            locations[job["jobid"]] = node_list
        LOGGER.info("System:find_job_location: locations are %s" % locations )
        return locations
    find_job_location = exposed(find_job_location)
    
    
    def find_queue_equivalence_classes(self, reservation_dict, \
                                                        active_queue_names):
        """
        Finds equivalent queues
        An equivalent queue is a queue which can run upon the same partition(s)
        For now, with one partition (everything!) this is irrelevant.
        Returns: equiv= [{'reservations': [], 'queues': ['default']}]
        """
        #LOGGER.debug("System:find queue equivalence classes" )
        equiv = []
        #print "Reservation_Dict is: %s" % reservation_dict
        #print "Active_queue_names is %s" % active_queue_names
        #print "Queue assignments are: %s" % self.queue_assignments
        for queue in self.queue_assignments:
            # skip queues that aren't running
            if not queue in active_queue_names:
                continue
            found_a_match = False
            #print "Heckle Queue is %s" % queue
            for equ in equiv:
                print "Heckle Equ is %s" % equ
                if equ['data'].intersection(self.queue_assignments[queue]):
                    equ['queues'].add(queue)
                    equ['data'].update(self.queue_assignments[queue])
                    found_a_match = True
                    break
            if not found_a_match:
                equiv.append({'queues': set([queue]),
                                'data': set(self.queue_assignments[queue]),
                                'reservations': set()})
        real_equiv = []
        for eq_class in equiv:
            found_a_match = False
            for equ in real_equiv:
                if equ['queues'].intersection(eq_class['queues']):
                    equ['queues'].update(eq_class['queues'])
                    equ['data'].update(eq_class['data'])
                    found_a_match = True
                    break
            if not found_a_match:
                real_equiv.append(eq_class)
        equiv = real_equiv
        for eq_class in equiv:
            for res_name in reservation_dict:
                for host_name in reservation_dict[res_name].split(":"):
                    if host_name in eq_class['data']:
                        eq_class['reservations'].add(res_name)
            for key in eq_class:
                eq_class[key] = list(eq_class[key])
            del eq_class['data']
        return equiv
        find_queue_equivalence_classes = exposed(find_queue_equivalence_classes)
    
    
    def get_partitions(self, locations):
        """
        Work-around to get the cqadm to run a single job on this system
        PRE:  locations is a list of dict of strings of possible node names
        POST:  if good, return locations
                if not good, raise exception and list bad nodes
        """
        logstr = "System:get_partition: "
        hiccup = HeckleConnector()
        heckle_node_set = set(hiccup.list_all_nodes())
        locs = locations[0]['name']
        LOGGER.debug( logstr + "raw is are: %s" % locations )
        LOGGER.debug( logstr + "vals are: %s" % locs )
        if type(locs) == ListType:
            locset = set(locs)
            badlocations = locset.difference(heckle_node_set)
            if badlocations:
                raise Exception(
        logstr + "Bad Locations: %s " % list(badlocations) )
        elif type(locs) == StringType:
            if locs not in locations:
                raise Exception( logstr + "Bad Locations: %s" % locs)
        else:
            raise Exception( logstr + 
"location needs to be string or list of strings, you provided %s : %s" \
% ( type(locs), locs))
        return locations
    get_partitions = exposed(get_partitions)
    
    

#!/usr/bin/python

import time, sys
from FTBEventPolicy import *

class EventMonitor():
    ftbEventPolicy = None
    policyDefinitions = {}

    def __init__(self):
	sHandle = self.doRegister()
	self.startMonitor(sHandle)

    def doRegister(self):
	self.ftbEventPolicy = FTBEventPolicy()
	sHandle = self.ftbEventPolicy.register('0.5',
                       'FTB.APP01.ES01',
                       'handler-01',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)
	return sHandle

    def startMonitor(self, sHandle):
	self.policyDefinitions = self.ftbEventPolicy.getPolicyDefinitions()

	while(True):
	    cEvent, ret = self.ftbEventPolicy.pollEvent(sHandle)
	    if ret != FTB_SUCCESS:
		continue
	    eventAttrib = self.policyDefinitions[cEvent.event_name]
            if eventAttrib.type == 'multiple':
		self.processMultipleTypeEvent(eventAttrib, cEvent)
            elif eventAttrib.type == 'timed':
		self.processTimedTypeEvent(eventAttrib, cEvent)
            else:
		self.processSingleTypeEvent(eventAttrib)

    def processSingleTypeEvent(self, ea):
	print 'Event: %s, Type: %s, Action: %s' % (
		        ea.name, ea.type, ea.action)


    def processMultipleTypeEvent(self, ea, ce):
	multipleCountThreshold = int(ea.count)
	if ((int(ce.seqnum) % multipleCountThreshold) == 0):
            print 'Received event %s of type \'%s\' from client \'%s\' on host: %s!' % (
		        ea.name, ea.type, ce.client_name, ce.incoming_src.hostname),
            print 'Application threshold reached. Taking action: %s' % ( ea.action )
            print 'client_jobid: %s, client_extension: %s, payload: %s' % (
			ce.client_jobid, ce.client_extension, ce.event_payload)
            print 'PID start_time: %s, PID: %s' % (
			ce.incoming_src.pid_starttime, ce.incoming_src.pid)
	

    timedEventMap = {}
    prevTime = None
    def processTimedTypeEvent(self, ea, ce):
	eventKey = ce.incoming_src.hostname + '-' + str(ce.incoming_src.pid)
#	print eventKey
	
	if not eventKey in self.timedEventMap:
#             print 'Event: %s, Type: %s, Action: %s, Time: %s' % (
# 		        ea.name, ea.type, ea.action, ea.time)
            curTime = time.time()
            if self.prevTime != None:
		deltaTime = curTime - self.prevTime
		self.prevTime = curTime
		deltaMiliSec = deltaTime * 1000
		if deltaMiliSec < int(ea.time):
                    print "*** At %d milisecs ***" % (deltaMiliSec)
		else:
                    print 'Caught but safe(%d msec)' % (deltaMiliSec)
            else:
		self.prevTime = curTime

if __name__=='__main__':
	EventMonitor()

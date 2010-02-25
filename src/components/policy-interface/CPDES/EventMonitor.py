#!/usr/bin/python

import time, sys
from FTBEventPolicy import *

class EventMonitor():
        schemaVersion = None
           clientName = None
           eventSpace = None
                jobID = None
    subscriptionStyle = None
             queueLen = 0

       ftbEventPolicy = None
    policyDefinitions = {}

    def __init__(self):
	
	sHandle = self.doRegister()
	self.startMonitor(sHandle)

    def loadConfiguration(self):
	

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
	print 'DEBUG: Event: %s, Type: %s, Action: %s' % (
		        ea.name, ea.type, ea.action)


    def processMultipleTypeEvent(self, ea, ce):
	multipleCountThreshold = int(ea.count)
	if ((int(ce.seqnum) % multipleCountThreshold) == 0):
            print 'DEBUG: Received event %s of type \'%s\' from client \'%s\' on host: %s!' % (
		        ea.name, ea.type, ce.client_name, ce.incoming_src.hostname),
            print 'DEBUG: Application threshold reached. Taking action: %s' % ( ea.action )
            print 'DEBUG: client_jobid: %s, client_extension: %s, payload: %s' % (
			ce.client_jobid, ce.client_extension, ce.event_payload)
            print 'DEBUG: PID start_time: %s, PID: %s' % (
			ce.incoming_src.pid_starttime, ce.incoming_src.pid)
	

    timedEventMap = {}
#    prevTime = None
    def processTimedTypeEvent(self, ea, ce):
	eventKey = ce.incoming_src.hostname + '-' + str(ce.incoming_src.pid)
        curTime = time.time()
#	print eventKey
	
	if not eventKey in self.timedEventMap:
            print 'DEBUG: First occurence of \'%s\'' % (eventKey)
            self.timedEventMap.update({eventKey: curTime})
	else:
            prevTime = self.timedEventMap[eventKey]
            deltaTime = curTime - self.timedEventMap[eventKey]
            self.timedEventMap[eventKey] = curTime
            deltaMiliSec = deltaTime * 1000
            if deltaMiliSec < int(ea.time):
                print 'DEBUG: *** At %d msecs ***' % (deltaMiliSec),
		print '. Taking action: %s.' % (ea.action) ,
		print ' for eventKey: %s' % (eventKey)
            else:
                print 'DEBUG: Caught event above threshold(%d msec). No action required.' % (
				deltaMiliSec), 

            print '. Threshold: %s msec' % (ea.time)


if __name__=='__main__':
	EventMonitor()

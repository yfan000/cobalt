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
		self.processTimedTypeEvent(eventAttrib)
            else:
		self.processSingleTypeEvent(eventAttrib)

    def processSingleTypeEvent(self, ea):
	print 'Event: %s, Type: %s, Action: %s' % (
		        ea.name, ea.type, ea.action)


    def processMultipleTypeEvent(self, ea, ce):
	multipleCountThreshold = int(ea.count)
	if ((int(ce.seqnum) % multipleCountThreshold) == 0):
            print 'Receved event %s of type %s from client %s on host: %s!' % (
		        ea.name, ea.type, ce.client_name, ce.incoming_src.hostname)
            print 'Application threshold reached. Taking action: %s' % ( ea.action )
	

    def processTimedTypeEvent(self, ea):
	print 'Event: %s, Type: %s, Action: %s' % (
		        ea.name, ea.type, ea.action)

if __name__=='__main__':
	EventMonitor()

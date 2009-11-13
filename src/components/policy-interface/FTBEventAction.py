#!/usr/bin/python

import xml.etree.cElementTree as ceTree
import string, sys, time
from ftb import *
from ctypes import *
from array import *

policyDirName  = '.'
policyFileName = 'policy.xml'
policyFile = open(policyDirName + '/' + policyFileName)

class EventTuple:
    def __init__(self, eventSpaceId, eventSpaceName, eventRule):
        self.eventSpaceId   = eventSpaceId
        self.eventSpaceName = eventSpaceName
        self.assignAllFields(eventRule)

    def assignAllFields(self, er):
        self.eventName     = er.find('eventName').text
        self.eventSeverity = er.find('eventSeverity').text
        self.eventAction   = er.find('eventAction').text

    def getEventName(self):
        return self.eventName

    def getEventSeverity(self):
        return self.eventSeverity

    def getEventAction(self):
        return self.eventAction


class EventsTable:
    eventTable = []

    def __init__(self):
        policyTree = ceTree.parse(policyFile)
	allSpace = [ space for space in policyTree.findall('eventSpace') ]
    	for eventSpace in policyTree.findall('eventSpace'):
            self.addToTable(eventSpace)
        
    def addToTable(self, es):
    	eventSpaceId   = es.get('eventSpaceId')
	eventSpaceName = es.get('eventSpaceName')
	for eventRule in es:
            self.eventTable.append(EventTuple(eventSpaceId, 
                                              eventSpaceName,          
                                              eventRule))
            
    def displayLoadedPolicies(self):
	for er in self.eventTable:
            print er.eventSpaceId, er.eventSpaceName, \
                er.getEventName(), er.getEventSeverity(), \
                er.getEventAction()

    def getEventsTable(self):
	return self.eventTable

class FTBEventAction(object):
    eventsTable = []

    def __init__(self):
	et = EventsTable()
	self.eventsTable = et.getEventsTable();
	self.bus = FTB()

    def register(self,
                 clientSchemaVer,
                 eventSpace,
                 clientName,
                 clientJobId,
                 clientSubscriptionStyle,
                 clientPollingQueueLen):

        self.bus.FTB_Connect(clientSchemaVer,
                             eventSpace,
                             clientName,
                             clientJobId,
                             clientSubscriptionStyle,
                             clientPollingQueueLen)

	eventSpaceEvents = self.getEventSpaceEvents(eventSpace)
	eventInfo = []
	for event in eventSpaceEvents:
            eventInfo.append([event.eventSpaceName,
                             event.eventSeverity])
	print eventInfo, len(eventInfo)

	self.bus.FTB_Declare_publishable_events( None, [ ["WATCH_DOG_EVENT", "INFO"] ], 1);

#	self.bus.FTB_Declare_publishable_events(None, eventInfo, len(eventInfo))
	sHandle = self.bus.FTB_subscribe_handle_t()
	self.bus.FTB_Subscribe( sHandle,
				'event_space=ftb.all.watchdog',
				None,
				None)

    def getEventSpaceEvents(self, eventSpaceName):
	eventSpaceEvents = []
	for event in self.eventsTable:
            if event.eventSpaceName == eventSpaceName:
		eventSpaceEvents.append(event)

	return eventSpaceEvents
    


if __name__=='__main__':
    et = FTBEventAction().getEventSpaceEvents('FTB.FTB_EXAMPLES.watchdog')

    for er in et:
        print er.eventSpaceId, er.eventSpaceName, \
            er.getEventName(), er.getEventSeverity(), \
            er.getEventAction()
    

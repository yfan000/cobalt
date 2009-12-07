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

    def getActionName(self, eventName):
	for eventTuple in self.eventTable:
            if eventTuple.eventName == eventName:
		return eventTuple.eventAction


class FTBEventAction(FTB):
    def __init__(self):
	self.et = EventsTable()
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

	sHandle = self.bus.FTB_subscribe_handle_t()
	self.bus.FTB_Subscribe( sHandle,
				'event_space=' + eventSpace,
				None,
				None)
	
	return sHandle

    def pollEvent(self, sHandle):
	cEvent = self.bus.FTB_receive_event_t()
	ret = self.bus.FTB_Poll_event(sHandle, cEvent)

	return cEvent, ret

    def getEventSpaceEvents(self, eventSpaceName):
	eventSpaceEvents = []
	for event in self.eventsTable:
            if event.eventSpaceName == eventSpaceName:
		eventSpaceEvents.append(event)

	return eventSpaceEvents
    
    def getEventAction(self, eventName):
	return self.et.getActionName(eventName)

if __name__=='__main__':
    et = FTBEventAction().getEventSpaceEvents('FTB.FTB_EXAMPLES.watchdog')

    for er in et:
        print er.eventSpaceId, er.eventSpaceName, \
            er.getEventName(), er.getEventSeverity(), \
            er.getEventAction()
    

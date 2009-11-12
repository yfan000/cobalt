#!/usr/bin/python

import xml.etree.cElementTree as ceTree

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

class FTBEventAction:
    def __init__(self):
	pass

    def register(self):
	pass

if __name__=='__main__':
    EventsTable().displayLoadedPolicies()

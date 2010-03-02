#!/usr/bin/python

import time, sys, os
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
	self.loadConfiguration()	
	sHandle = self.doRegister()
	self.startMonitor(sHandle)

    def loadConfiguration(self):
	configFile = open(configFileName, 'r')
	configTree = ceTree.parse(configFile)

	policyFile = None
	for config in configTree.findall("monitorConfig"):
            if config.get('id') != configInstance:
		continue
            
            for param in config.findall('policyFilePath'):
		policyFile = open(param.text, 'r')
	    
	    self.policyFilePath    = config.find('policyFilePath').text
	    self.schemaVersion     = config.find('schemaVersion').text 
	    self.clientName        = config.find('clientName').text
	    self.clientJobID       = config.find('clientJobID').text 
	    self.namespaceString   = config.find('namespaceString').text 
	    self.subscriptionStyle = config.find('subscriptionStyle').text 
	    self.pollingQueueLen   = int(config.find('pollingQueueLen').text)
			
	return policyFile	
	

    def doRegister(self):
	self.ftbEventPolicy = FTBEventPolicy(self.policyFilePath)
	sHandle = self.ftbEventPolicy.register(self.schemaVersion,
                       			       self.namespaceString,
					       self.clientName,
					       self.clientJobID,
					       self.subscriptionStyle,
					       self.pollingQueueLen)
	return sHandle

    def startMonitor(self, sHandle):
	self.policyDefinitions = self.ftbEventPolicy.getPolicyDefinitions()

	while(True):
	    cEvent, ret = self.ftbEventPolicy.pollEvent(sHandle)
	    if ret != FTB_SUCCESS:
		continue
	    eventAttrib = self.policyDefinitions[cEvent.event_name]
            if eventAttrib.eventType == 'MULTIPLE':
		self.processMultipleTypeEvent(eventAttrib, cEvent)
            elif eventAttrib.eventType == 'TIMED':
		self.processTimedTypeEvent(eventAttrib, cEvent)
            elif eventAttrib.eventType == 'HYBRID':
		self.processHybridTypeEvent(eventAttrib, cEvent)
            else:
		self.processSingleTypeEvent(eventAttrib)


    def printDebug(self, ea, ce):
	print '(LOG) Sequence Number: %s' % ce.seqnum
	print '(LOG) Event: %s' % ea.name
	print '(LOG) EventType: %s' % ea.eventType
	print '(LOG) Client Name: %s' % ce.client_name
	print '(LOG) Host: %s' % ce.incoming_src.hostname
	print '(LOG) JobID: %s' % ce.client_jobid
	print '(LOG) Client Extension: %s' % ce.client_extension
	print '(LOG) Payload: %s' % ce.event_payload
	print '(LOG) PID: %s' % ce.incoming_src.pid
	print '(LOG) Process start time: %s' % ce.incoming_src.pid_starttime

        if ea.actionType == 'MESSAGE':
            print '(LOG) Action: %s' % (ea.action)
        elif ea.actionType == 'EXECUTION':
            os.system(ea.action)

	print


    def processSingleTypeEvent(self, ea):
	print 'DEBUG: Event: %s, Type: %s, ActionType: %s' % (
                	ea.name, ea.eventType, ea.actionType),

    	if ea.actionType == 'MESSAGE':
            print ' Action: %s' % (ea.action)
    	elif ea.actionType == 'EXECUTION':
            os.popen(ea.action)


    def processMultipleTypeEvent(self, ea, ce):
	multipleCountThreshold = int(ea.count)
	if ((int(ce.seqnum) % multipleCountThreshold) == 0):
            self.printDebug(ea, ce)
	

    timedEventMap = {}
    def processTimedTypeEvent(self, ea, ce):
	eventKey = ce.incoming_src.hostname + '-' + \
			 str(ce.incoming_src.pid) + '-' + ea.name 

        curTime = time.time()
	
	if not eventKey in self.timedEventMap:
            print 'DEBUG: First occurence of \'%s\'' % (eventKey)
            self.timedEventMap.update({eventKey: curTime})
	else:
            prevTime = self.timedEventMap[eventKey]
            deltaTime = curTime - self.timedEventMap[eventKey]
            self.timedEventMap[eventKey] = curTime
            deltaMiliSec = deltaTime * 1000
            if deltaMiliSec < int(ea.time):
		print '(LOG) eventKey: %s' % (eventKey)
		self.printDebug(ea, ce)


    hybridEventTimeLists = {}
    def processHybridTypeEvent(self, ea, ce):
	eventKey = ce.incoming_src.hostname + '-' + \
			 str(ce.incoming_src.pid) + '-' + ea.name 
        curTime = time.time()
	
	if not eventKey in self.hybridEventTimeLists:
            print '(LOG) First occurence of \'%s\'' % (eventKey)
            newTimeList = []
            newTimeList.append(curTime)
            self.hybridEventTimeLists.update({eventKey: newTimeList})
            self.timedEventMap.update({eventKey: curTime})
	else:
            thisEventTimeList = self.hybridEventTimeLists[eventKey]
            thisEventTimeList.append(curTime)
            curIdx = len(thisEventTimeList) - 1
            trackStartTimeIdx = curIdx - int(ea.count)

	    if trackStartTimeIdx < 0:
		return
            
            deltaTime = curTime - thisEventTimeList[trackStartTimeIdx]
            deltaMiliSec = deltaTime * 1000
            if deltaMiliSec < int(ea.time):
                self.timedEventMap[eventKey] = curTime
		print '(LOG) eventKey: %s' % (eventKey)
		print '(LOG) deltaTime: %d' % (deltaMiliSec)
		self.printDebug(ea, ce)


if __name__=='__main__':
	EventMonitor()

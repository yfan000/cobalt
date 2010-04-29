#/**********************************************************************************/
#/* FTB:ftb-info */
#/* This file is part of FTB (Fault Tolerance Backplance) - the core of CIFTS
# * (Co-ordinated Infrastructure for Fault Tolerant Systems)
# * See http://www.mcs.anl.gov/research/cifts for more information.
# *
# * This file provides FTB bindings for python. This will be automatically generated 
# * using a script in the future
# *
#*/
#/* FTB:ftb-info */
#/* FTB:ftb-fillin */
#/* FTB_Version: Will be included in follow-up version to 0.6
# * FTB_API_Version: 0.5
# * FTB_Heredity:FOSS_ORIG
# * FTB_License:BSD
#*/
#/* FTB:ftb-fillin */
#/* FTB:ftb-bsd */
#/* This software is licensed under BSD. See the file FTB/misc/license.BSD for
# * complete details on your rights to copy, modify, and use this software.
#*/
#/* FTB:ftb-bsd */
#/**********************************************************************************/

from ctypes import *
import string, sys, time

libftb = cdll.LoadLibrary("libftb.so")


#declare the return values for FTB
FTB_SUCCESS = 0
FTB_ERR_GENERAL = -1
FTB_ERR_EVENTSPACE_FORMAT = -2
FTB_ERR_SUBSCRIPTION_STYLE = -3
FTB_ERR_INVALID_VALUE = -4
FTB_ERR_DUP_CALL = -5
FTB_ERR_NULL_POINTER = -6
FTB_ERR_NOT_SUPPORTED = -7
FTB_ERR_INVALID_FIELD = -8
FTB_ERR_INVALID_HANDLE = -9
FTB_ERR_DUP_EVENT = -10
FTB_ERR_INVALID_SCHEMA_FILE = -11
FTB_ERR_INVALID_EVENT_NAME = -12
FTB_ERR_INVALID_EVENT_TYPE = -13
FTB_ERR_SUBSCRIPTION_STR = -14
FTB_ERR_FILTER_ATTR = -15
FTB_ERR_FILTER_VALUE = -16
FTB_GOT_NO_EVENT = -17
FTB_FAILURE = -18
FTB_ERR_INVALID_PARAMETER = -19
FTB_ERR_NETWORK_GENERAL = -20
FTB_ERR_NETWORK_NO_ROUTE = -21

#/* If client will subscribe to any events */
FTB_SUBSCRIPTION_NONE = 0
#/* If client plans to poll - a polling queue is created */
FTB_SUBSCRIPTION_POLLING = 1
#/* If client plans to use callback handlers */
FTB_SUBSCRIPTION_NOTIFY = 2

FTB_ERR_HANDLE_NONE = 0
FTB_ERR_HANDLE_NOTIFICATION = 1
FTB_ERR_HANDLE_RECOVER = 2

# Python Structures defined for C structures in ftb_def.h

class FTB_location_id_t(Structure):
        _fields_ = [('hostname', c_char * 64),
                    ('pid_starttime', c_char * 32),
                    ('pid', c_uint32)] 

class FTB_client_id_t(Structure):
        _fields_ = [('region', c_char * 64),
                                ('comp_cat', c_char * 64),
                                ('comp', c_char * 64),
                                ('client_name', c_char * 16),
                                ('ext', c_uint8)]

class FTB_client_handle_t_con(Structure):
        _fields_ = [('valid', c_uint8),
                                ('client_id', FTB_client_id_t)]
class FTB_event_t(Structure):
        _fields_ = [('region', c_char * 64),
                        ('comp_cat', c_char * 64),
                            ('comp', c_char * 64),
                                ('event_name', c_char * 32),
                        ('severity', c_char * 16),
                        ('client_job_id', c_char * 16),
                            ('client_name', c_char * 16),
                                ('hostname', c_char * 64),
                        ('seqnum', c_uint16),
                        ('event_type', c_uint8),
                            ('event_payload', c_char * 368)]


class FTB_client_t(Structure):
        _fields_ = [('client_schema_ver', c_char * 8),
                    ('event_space', c_char * 64),
                    ('client_name', c_char * 16),
                    ('client_jobid', c_char * 16),
                    ('client_subscription_style', c_char * 32),
                    ('client_polling_queue_len', c_uint64)]
   
class FTB_event_info_t_con(Structure):
        _fields_ = [('event_name', c_char * 32),
                    ('event_severity', c_char * 16)]
    
class FTB_event_properties_t(Structure):
        _fields_ = [('event_type', c_char),
                    ('event_payload', c_char * 368)]
    
    
class FTB_receive_event_t(Structure):
        _fields_ = [('event_space', c_char * 64),
                    ('event_name', c_char * 32),
                    ('severity', c_char * 16),
                    ('client_jobid', c_char * 16),
                    ('client_name', c_char * 16),
                    ('client_extension', c_uint8),
                    ('seqnum', c_uint16),
                    ('incoming_src', FTB_location_id_t),
                    ('event_type', c_uint8),
                    ('event_payload', c_char * 368)]
    
    # Python Structures defined for C structures in ftb_client_lib_defs.h.
    
class FTB_id_t(Structure):
        _fields_ = [('location_id', FTB_location_id_t),
                                ('client_id', FTB_client_id_t)]
    
    
    # Below Python Structures are defined for C structures in
    # ftb_client_lib_defs.h and typedefed in ftb_def.h
    
class FTB_subscribe_handle_t_con(Structure):
        _fields_ = [('client_handle', FTB_client_handle_t_con),
                        ('subscription_event', FTB_event_t),
                        ('subscription_type', c_uint8),
                            ('valid', c_uint8)]
    
class FTB_event_handle_t_con(Structure):
        _fields_ = [('event_name', c_char * 32),
                        ('severity', c_char * 16),
                        ('client_id', FTB_client_id_t),
                            ('seqnum', c_uint16),
                            ('location_id', FTB_location_id_t)]



class FTB(object):


    def __init__(self):
	    self.handle = FTB_client_handle_t_con()
	    #self.shandle = FTB_subscribe_handle_t_con()
	    #self.receive_event = FTB_receive_event_t_con()
            #self.event_handle = FTB_event_handle_t("")
              

    def FTB_client_handle_t(self):
	return self.handle

    def FTB_subscribe_handle_t(self):
	return FTB_subscribe_handle_t_con()

    def FTB_receive_event_t(self):
	return FTB_receive_event_t()
    
    def FTB_event_handle_t(self):
	return FTB_event_handle_t_con()

    #def FTB_event_info_t(self,event_info_arg):
	#self.event_info=[]
        #for i in range(len(event_info_arg)):
         #      self.event_info.append(FTB_event_info_t_con(*event_info_arg[i]))
	#return self.event_info

    def FTB_Connect(self, client_schema_ver, event_space, client_name, client_jobid, client_subscription_style="FTB_SUBSCRIPTION_POLLING", client_polling_queue_len = 0):
        self.cinfo = FTB_client_t()
        self.cinfo.client_schema_ver =  client_schema_ver
        self.cinfo.event_space = event_space
        self.cinfo.client_name = client_name
        self.cinfo.client_jobid = client_jobid
        self.cinfo.client_subscription_style = client_subscription_style
        self.cinfo.client_polling_queue_len = client_polling_queue_len
        self.ret = libftb.FTB_Connect(byref(self.cinfo), byref(self.handle))
        if self.ret != FTB_SUCCESS:
            print "Could not connect to FTB. Return code =", self.ret
        return self.ret

    def FTB_Declare_publishable_events(self,schema_file, event_info, num_events):
        #self.handle = handle
        #self.schema_file = schema_file
        #self.event_info = event_info
        #self.num_events = num_events
	
	#event_info_array_type_pointer = FTB_event_info_t_con*num_events
	
	#event_info_array_instance=event_info_array_type_pointer()
	#i=0
	#for pt in event_info_array_instance:
	#	pt=FTB_event_info_t_con(*event_info[i])
	#	print pt.event_name
	#	i=i+1
        #self.ret = libftb.FTB_Declare_publishable_events(self.handle, self.schema_file, byref(event_info_array_instance ),self.num_events)
	#if self.ret != FTB_SUCCESS:
	#		print "FTB_Declare_Publishable_events is not successful. Return code =", self.ret
	#		sys.exit()
	#		return self.ret
	if event_info !=None:
		for i in range(0,num_events):
			self.ret = libftb.FTB_Declare_publishable_events(self.handle, schema_file, byref(FTB_event_info_t_con(*event_info[i])), 1)
			if self.ret != FTB_SUCCESS:
				print "FTB_Declare_Publishable_events is not successful. Return code =", self.ret
				sys.exit()
				return self.ret
	else:
		self.ret = libftb.FTB_Declare_publishable_events(self.handle, schema_file, None, num_events)

# User needs to be returned the event handle
    def FTB_Publish(self, event_name, event_handle,event_payload="", event_type=0):
        event_properties = FTB_event_properties_t()
        #self.event_handle = event_handle
        if (event_type == 0 and event_payload == ""):
            ret = libftb.FTB_Publish(self.handle, event_name, 0, byref(event_handle))
        else:
#            event_properties.event_type = event_type
#            event_properties.event_payload = event_payload
            ret = libftb.FTB_Publish(self.handle, self.event_name, byref(self.event_properties), byref(event_handle))
        if ret != FTB_SUCCESS:
            print "FTB_Publish failed. Return code=", self.ret
            sys.exit()
        return ret

#User needs to be returned the subscribe handle
    def FTB_Subscribe(self, shandle,subscription_str, callback_func, callback_args):
        #self.handle = handle
	#self.shandle = shandle
        #self.subscription_str = subscription_str
        #self.callback_func = callback_func
        #self.callback_args = callback_args
        #self.ret = libftb.FTB_Subscribe(byref(self.shandle), self.handle, self.subscription_str, self.callback_func, self.callback_args)
	
	ret = libftb.FTB_Subscribe(byref(shandle), self.handle, subscription_str, callback_func, callback_args)
        if ret != FTB_SUCCESS:
            print "FTB_Subscribe failed. Return code =", ret
            sys.exit()
        return ret

    def FTB_Unsubscribe(self, shandle):
        #self.shandle = shandle
        ret = libftb.FTB_Unsubscribe(byref(shandle))
        if ret != FTB_SUCCESS:
            print "FTB_Subscribe failed. Return code =", ret
            sys.exit()
        return ret

    def FTB_Poll_event(self,shandle,receive_event):
        #self.receive_event = receive_event
        #self.shandle = shandle
        #self.ret = libftb.FTB_Poll_event(self.shandle, byref(self.receive_event))
        ret = libftb.FTB_Poll_event(shandle, byref(receive_event))
#	if ret != FTB_SUCCESS:
#            print "FTB_Poll_event failed. Return code =", self.ret
#            sys.exit()
        return ret

    def FTB_Disconnect(self):
#        self.handle = handle
        ret = libftb.FTB_Disconnect(self.handle)
        if ret != FTB_SUCCESS:
            print "FTB_Disconnect failed. Return code =", self.ret
            sys.exit()
        return ret

    def FTB_Get_event_handle(self, receive_event, event_handle):
        self.receive_event = receive_event
        self.event_handle = event_handle
        self.ret = libftb.FTB_Get_event_handle(self.receive_event, byref(event_handle))
        if self.ret != FTB_SUCCESS:
            print "FTB_Get_event_handles failed. Return code =", self.ret
            sys.exit()
        return self.ret

    def FTB_Compare_event_handle(self, event_handle1, event_handle2):
        self.event_handle1 = event_handle1
        self.event_handle2 = event_handle2
        seld.ret = libftb.FTB_Compare_event_handle(self.event_handle1, event_handle2)
        if self.ret != FTB_SUCCESS:
            print "FTB_Compare_event_handles failed. Return code =", self.ret
            sys.exit()
        return self.ret

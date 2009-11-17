#!/usr/bin/python

import time
from FTBEventAction import *

fea = FTBEventAction()
sHandle = fea.register('0.5',
                       'FTB.FTB_EXAMPLES.watchdog',
                       'trial-watchdog',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)


eHandle = fea.FTB_event_handle_t()
fea.raiseEvent("WATCH_DOG_EVENT",eHandle)


#!/usr/bin/python

from FTBEventAction import *
     
fea = FTBEventAction()
fea.register('0.5',
                          'FTB.FTB_EXAMPLES.watchdog',
                          'trial-watchdog',
                          '0',
                          'FTB_SUBSCRIPTION_POLLING',
                          0)

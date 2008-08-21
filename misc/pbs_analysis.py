'''
Metrics Manual:

This script prints various scheduler metrics for a given Cobalt PBS 
log file.  

To run, type:  python metrics.py <number_of_racks> <log_file>
    
    Options (in any order): -p                          [print all job info]
                            -j <job id>                 [print info for jobid]
                            --day-util                  [print utilization for each day]


For graphing, Gnuplot.py must be installed.

'''

import sys
from math import sqrt
import re
from datetime import datetime
from time import ctime
import time

from Numeric import *
import Gnuplot

sarg = ' '.join(sys.argv)

class SchedMetrics(object):

    def sched_metrics(self):
        date_re = re.compile("\d\d/\d\d/\d\d\d\d")
        jobid_Ere = re.compile(";E;\d+;")
        jobid_Sre = re.compile(";S;\d+;")

        linelist = [line for line in open(sys.argv[2]) ]

        first = re.findall(date_re, linelist[0])
        mdy_start = first[0].split("/")
        mdy_start = [int(mdy_start[num]) for num in range(len(mdy_start))]

        last = re.findall(date_re,  linelist[len(linelist)-1]) 
        mdy_end = last[0].split("/")
        mdy_end = [int(mdy_end[num]) for num in range(len(mdy_end))]

        #calc start and end of log.  Ignore times, only consider 24 hour periods 
        #datetime(year, month, day)
        start = datetime(mdy_start[2], mdy_start[0], mdy_start[1], 0, 0, 0)
        finish = datetime(mdy_end[2], mdy_end[0], mdy_end[1], 0, 0, 0)

        self.log_days = ((finish - start).days + 1)       
        self.log_secs = self.log_days*60*60*24

        self.log_start_time = time.mktime(start.timetuple())
        self.log_end_time = time.mktime(finish.timetuple()) + 60*60*24
      
        self.ejobs = {}
        self.sjobs = {}

        self.endlist = []

        jobid = '' 
        for line in linelist:
            if line.find(";E;") is not -1 or line.find(";S;") is not -1:
        
                if line.find(";E;") is not -1:
                    s = line.find("start=") + 6
                    e = line.find(" ", s)
                    start = float(line[s:e]) 

                    s = line.find("end=") + 4
                    e = line.find(" ", s)
                    end = float(line[s:e])

                    s = line.find("nodect=") + 7
                    e = line.find(" ", s)
                    nodect = float(line[s:e])

                    s = line.find("qtime=") + 6
                    e = line.find(" ", s)
                    qtime = float(line[s:e])
                            
                    self.walltime = end - start
                    #self.endlist.append(end)

                    ejobid = re.findall(jobid_Ere, line)[0]
                    num = ejobid.rfind(";")
                    jobid = ejobid[3:num]                   

                    self.ejobs.update( {jobid:{}} )
                                        
                    self.ejobs[jobid].update( {'end': end,
                                             'start': start,
                                          'walltime': end - start,
                                            'nodect': nodect,
                                             'qtime': qtime,
                                           'metrics': {'responsetime': end - qtime,
                                                           'waittime': start - qtime }
                                             } )

                elif line.find(";S;") is not -1:  
                    sjob = re.findall(jobid_Sre, line)[0]
                    num = sjob.rfind(";")
                    jobid = sjob[3:num]
                 
                    s = line.find("start=") + 6
                    e = line.find(" ", s)
                    start = float(line[s:e]) 
        
                    end = None                

                    s = line.find("nodect=") + 7
                    e = line.find(" ", s)
                    nodect = float(line[s:e])

                    s = line.find("qtime=") + 6
                    e = line.find(" ", s)
                    qtime = float(line[s:e])

                    self.sjobs.update( {jobid:{}} )
                                        
                    self.sjobs[jobid].update( {'end': end,
                                             'start': start,
                                            'nodect': nodect,
                                             'qtime': qtime,
                                           'metrics': {'responsetime': None,
                                                           'waittime': start - qtime }
                                             } )
                

           
        self.util = self.compute_log_util()  #essential for ALL metrics
        self.day_util_list = self.compute_day_util_list()

        print max(self.wait_time_list), max(self.response_time)
        self.print_metrics()        
    
        self.graph_metrics()        

    def compute_utilization_percent(self, util, log_secs):
        return ( (util)/(float(sys.argv[1])*1024*log_secs) ) * 100
    
    def compute_mean(self, lvalues):
        return sum(lvalues)/len(lvalues)

    def compute_variance(self, lvalues):
        mean = self.compute_mean(lvalues)
        return sum( [(lvalues[i] - mean)**2 for i in range(len(lvalues))] ) / len(lvalues)
        
    def compute_stddeviation(self, lvalues):
        return sqrt(self.compute_variance(lvalues))
    
    def compute_median(self, lvalues):
        lvalues = sorted(lvalues)
        if (len(lvalues) % 2 == 0):
            return (lvalues[len(lvalues)/2] + lvalues[len(lvalues)/2 - 1])  / 2.0   
        else:
            return lvalues[len(lvalues)/2]

    def compute_log_util(self):
        self.wait_time_list = []
        self.response_time = []
        util = 0
        self.graph_list = []
        for x in self.sjobs.keys():
            if x not in self.ejobs.keys():
                util += self.sjobs[x]['nodect'] * (self.log_end_time - self.sjobs[x]['start'])
                self.graph_list.append( (self.sjobs[x]['start'], 
                                         self.sjobs[x]['metrics']['waittime'],
                                         None) )
        for x in self.ejobs.keys():
            if x not in self.sjobs.keys():
                util += self.ejobs[x]['nodect'] * (self.ejobs[x]['end'] - self.log_start_time)
                #self.response_time.append( self.ejobs[x]['end'] - self.ejobs[x]['qtime'] )
                self.graph_list.append( (self.ejobs[x]['start'], 
                                         self.ejobs[x]['metrics']['waittime'],
                                         self.ejobs[x]['metrics']['responsetime'] ) )

            else:
                util += self.ejobs[x]['nodect'] * self.ejobs[x]['walltime']

                self.graph_list.append((self.ejobs[x]['start'], 
                                        self.ejobs[x]['metrics']['waittime'],
                                        self.ejobs[x]['metrics']['responsetime']) )

        self.wait_time_list = [self.graph_list[x][1] for x in range(len(self.graph_list))]
        self.response_time = [self.graph_list[x][2] for x in range(len(self.graph_list))
                              if self.graph_list[x][2] is not None] 
        return util

    def compute_day_util_list(self):
        sdaydict = {}
        edaydict = {} 
        daylist = []
        
        day_util_list = []
        day_time = self.log_start_time
        for t in range(self.log_days):
            daylist.append(day_time)
            day_time +=  60*60*24
            sdaydict.update({t:{}})
            edaydict.update({t:{}})
      
        for day in sdaydict.keys():
          for x in self.sjobs.keys():
             if  daylist[day] <= self.sjobs[x]['start'] < daylist[day] + 60*60*24:
                sdaydict[day].update({x:self.sjobs[x]})
          for x in self.ejobs.keys():
             if  daylist[day] <= self.ejobs[x]['end'] < daylist[day] + 60*60*24:
                edaydict[day].update({x:self.ejobs[x]})

       
        temputil = 0 
        for day in sdaydict.keys():
          day_start = daylist[day]            
          day_end = day_start + 60*60*24 
          for x in sdaydict[day].keys():
            if x not in edaydict[day].keys():
                temputil += sdaydict[day][x]['nodect'] * (day_end  - sdaydict[day][x]['start'])

          for x in edaydict[day].keys():
            if x not in sdaydict[day].keys():
                temputil += edaydict[day][x]['nodect'] * (edaydict[day][x]['end'] - day_start)
            else:
                temputil += edaydict[day][x]['nodect'] * edaydict[day][x]['walltime']
          day_util_list.append( self.compute_utilization_percent(temputil, 60*60*24) )
          
          temputil = 0
        return day_util_list

    def print_metrics(self):
        lookfor = re.findall('-p', sarg)
        if lookfor and lookfor[0] in sarg:
          print "jobid qtime         startime      endtime        waittime       responsetime"
          print "============================================================================"
          for x in self.sjobs.keys():
            
            print "start", x, self.sjobs[x]['qtime'], self.sjobs[x]['start'],  \
                  self.sjobs[x]['end'], self.sjobs[x]['metrics']['waittime'],  \
                  self.sjobs[x]['metrics']['responsetime']

          print "============================================================================"
          print "jobid qtime         startime      endtime        waittime       responsetime"


          print "jobid qtime         startime      endtime        waittime       responsetime"
          print "============================================================================"
          for x in self.ejobs.keys():
            
            print "END", x, self.ejobs[x]['qtime'], self.ejobs[x]['start'],  \
                  self.ejobs[x]['end'], self.ejobs[x]['metrics']['waittime'],  \
                  self.ejobs[x]['metrics']['responsetime']

          print "============================================================================"
          print "jobid qtime         startime      endtime        waittime       responsetime"
        
        print "\n\n\n"
        print "***********************************************************************"
        print "*                             metrics.py                              *"
        print "***********************************************************************"
        print "Log Period Given:", ctime(self.log_start_time), "-", ctime(self.log_end_time)
        print 
        print "-----------------------------Utilization-------------------------------"
        print "%-23s" % "Utilization:",self.compute_utilization_percent(self.util, self.log_secs),"%"
        print
        print "----------------------------- Wait Time -------------------------------"
        print "%-23s" % "Mean wait time:",  self.compute_mean(self.wait_time_list)
        print "%-23s" % "Median wait time:", self.compute_median(self.wait_time_list)
        print "%-23s" % "Variance:", self.compute_variance(self.wait_time_list)
        print "%-23s" % "Std Deviation:", self.compute_stddeviation(self.wait_time_list)
        print
        print "--------------------------- Response Time -----------------------------"
        print "%-23s" % "Mean response time:", self.compute_mean(self.response_time)
        print "%-23s" % "Median response time:", self.compute_median(self.response_time)
        print "%-23s" % "Variance:", self.compute_variance(self.response_time)
        print "%-23s" % "Std Deviation:", self.compute_stddeviation(self.response_time)
        print

        lookfor = re.findall('-j +\d*', sarg)
        if lookfor and lookfor[0] in sarg: 
            jobid = lookfor[0].split(' ')[1]

            print "START RECORD"
            print "jobid qtime         startime      endtime        waittime       responsetime"
            print "============================================================================"
            print  self.sjobs[jobid]['qtime'], self.sjobs[jobid]['start'],  \
                   self.sjobs[jobid]['end'], self.sjobs[jobid]['metrics']['waittime'],  \
                   self.sjobs[jobid]['metrics']['responsetime']
            print
            print "END RECORD"
            print "jobid qtime         startime      endtime        waittime       responsetime"
            print "============================================================================"
            print  self.ejobs[jobid]['qtime'], self.ejobs[jobid]['start'],  \
                   self.ejobs[jobid]['end'], self.ejobs[jobid]['metrics']['waittime'],  \
                   self.ejobs[jobid]['metrics']['responsetime']
            print

        lookfor = re.findall('--day-util', sarg)
        if lookfor and lookfor[0] in sarg:
            for x in range(len(self.day_util_list)):
                if self.day_util_list[x]: 
                    blah = "Day "+ str(x) + ":"
                    print "%-10s" % blah,  self.day_util_list[x]
        print "\nMax Utilization Day:", max(self.day_util_list)
        print "Min Utilization Day:", min(self.day_util_list)        

    def graph_metrics(self):
        g = Gnuplot.Gnuplot()
    
        self.graph_list.sort()

        x = [ self.graph_list[num][0] for num in range(len(self.graph_list)) ]
        y = [ self.graph_list[num][1] for num in range(len(self.graph_list)) ]
        
        d = Gnuplot.Data(x,y, title='Wait Time', with='line')
        g.xlabel('start time')
        g.ylabel('Wait Time (secs)')                   
        g.plot(d)                         
        g.hardcopy('wait_time.ps', enhanced=1, color=1)

        raw_input("\nPress key...\n")

        x1 = [ self.graph_list[num][0] for num in range(len(self.graph_list)) 
                if self.graph_list[num][2] != None]
        y2 = [ self.graph_list[num][2] for num in range(len(self.graph_list)) 
                if self.graph_list[num][2] != None]

        d = Gnuplot.Data(x1, y2, title='Response Time', with='line')
        g.xlabel('start time')
        g.ylabel('Response Time (secs)')                   
        g.plot(d)                         
        g.hardcopy('response_time.ps', enhanced=1, color=1)
        
        raw_input("\nPress key...\n")
        x = range(len(self.day_util_list))
        y = self.day_util_list
        d = Gnuplot.Data(x,y, title='Utilization', with='line')
        g.xlabel('Day')
        g.ylabel('Time (secs)')                   
        g.plot(d)
        g.hardcopy('utilization_by_day.ps', enhanced=1, color=1)

        raw_input("\nPress key to end...\n")


  
schedmetrics = SchedMetrics()
schedmetrics.sched_metrics()





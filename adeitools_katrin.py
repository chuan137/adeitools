import sys, traceback
import csv
import time 
import datetime
import calendar
import urllib
import re
import xml.etree.ElementTree as ET
import numpy as np
import colorstring

CACHE_ = 0
DEBUG_ = 0

def print_exc():
    exctype, value, tb  = sys.exc_info()
    filename, linenumber, functionname, text =  traceback.extract_tb(tb, 1)[0]
    sys.stderr.write("[Error] Unexpected exception\n")
    sys.stderr.write("            type: %s\n" % exctype)
    sys.stderr.write("         message: %s\n" % colorstring.ERROR(value.message))
    sys.stderr.write("        function: %s\n" % colorstring.ERROR(functionname))
    sys.stderr.write("            text: %s\n" % text)
    sys.stderr.write("            file: %s: %s\n" % (filename, linenumber))


def utcsecond(s):
    """
        string "s" specifies a date in format "day-Month-year hour:minute:second"
        this function converts "s" into utc second since epoch
    """
    slist = s.split()
    day, month, year = slist[0].split('-')
    hour, minute, second = slist[1].split(':')

    timestr = "%s " % year
    timestr += "%s " % month
    timestr += "%s " % day
    timestr += "%s " % hour
    timestr += "%s " % minute
    timestr += "%s" % int(float(second)+0.5)

    return calendar.timegm(time.strptime(timestr, "%y %b %d %H %M %S"))

def secSinceEpoch(d):
    """ 
        seconds since epoch 
    """
    epoch = datetime.datetime(1970,1,1)
    return int((d-epoch).total_seconds()+0.5)

def xmlparser(handler):
    tree = ET.parse(handler)
    result = []
    for child in tree.getroot():
        result.append(child.attrib)
    return result 

def csvparser(handler):
    rows = csv.reader(handler, skipinitialspace=True)                                                                      
    fields = rows.next()
    data = [row for row in rows if row]
    return zip(fields, *data)

def dictvalues(dlist, tag):
    return [d[tag] or '' for d in dlist if tag in d.keys()]


class adeiReader:
    def __init__(self):
        self.host = ''
        self.name = ''
        self.server = ''
        self.groups = {}
        self.sensors = []
        self.today = datetime.datetime(2013,5,30)

    def setup(self, db_host, db_server, db_name):
        self.groups = {}
        self.name = db_name
        self.server = db_server
        self.host = db_host

        self.query_template = \
            "http://%s/services/%s?db_server=%s&db_name=%s"\
            % (db_host, '%s', db_server, db_name)

        try:
            groups = [d.get('db_group') for d in self.listGroup()]
            if not groups: 
                raise RuntimeError("no group found")

            for g in groups:
                #date = self.groupLastStamp(g)
                sensorlist = self.listSensor(g)

                sensors = { int(s['value']): {
                                'id':           s.get('value'),
                                'uid':          s.get('uid', ''),
                                'name':         s.get('name') } 
                          for s in sensorlist}

                self.groups[g] = {
                        'stamp': None,
                        'sensors': sensors
                }

                for k, v in sensors.iteritems():
                    v['id'] = k
                    v['group'] = g
                    self.sensors.append(v)

            if DEBUG_:
                for k, v in self.groups.iteritems():
                    print k
                    for s, ss in sorted(v['sensors'].items()):
                        print s, ss

        except IOError:
            print_exc()
            sys.exit(0)
        except RuntimeError:
            print_exc()
            sys.exit(0)
        

    def listGroup(self):
        url = self.query_template % "list.php" + "&target=groups"
        return xmlparser(urllib.urlopen(url))


    def listSensor(self, group):
        res = []
        url = self.query_template % "list.php" + "&target=items"
 
        for g in [group] or self.groups.keys():
            url += "&db_group=%s&info=0" % g
            res.extend( xmlparser(urllib.urlopen(url)) )
        return res
        

#    def updateSensor(self, newkey, attr):
#        for g in dictvalues(self.groups, 'name'):
#            ids = dictvalues(self.listSensor([g]), 'value')
#            attrs = dictvalues(self.listSensor([g]), attr)
#
#            for id_, a_ in zip(ids, attrs):
#                for i in range(len( self.sensors )):
#                    if id_ == self.sensors[i]['id'] and g == self.sensors[i]['group']:
#                        self.sensors[i][newkey] = a_


    def groupLastStamp(self, group, fmt='sec'):
        url = self.query_template % "getdata.php" + "&db_group=%s&window=-1" % group
        if CACHE_:
            url += "&cache=0"
        dates = csvparser(urllib.urlopen(url))[0]
        res = datetime.datetime.strptime(
                dates[1], "%d-%b-%y %H:%M:%S.%f") if dates[1:] \
                        else self.today

        if fmt == 'sec':
            return secSinceEpoch(res)
        else:
            return res


    def groupLastDayStamp(self, group, fmt='sec'):
        date = self.groupLastStamp(group, fmt='date')
        res = datetime.datetime(date.year, date.month, date.day)
        if fmt == 'sec':
            return secSinceEpoch(res)
        else:
            return res


    def sensorFilter(self, sensorName, groupName):
        pattern = "^%s.[0-9]{3}$" % sensorName
        prog = re.compile(pattern)
        pattern2 = "^%s$" % sensorName
        prog2 = re.compile(pattern2)

        res = []
        for i, sensor in self.groups[groupName]['sensors'].iteritems():
            if prog.match(sensor['name']) or prog2.match(sensor['name']):
                res.append(i)
        return res


    def getSensorData(self, id_, groupname, starttime, deltatime=86400, resample=10):
        endtime = starttime + deltatime
        
        url =  self.query_template % "getdata.php"
        url += "&db_group=%s" % groupname
        url += "&db_mask=%s" % id_
        url += "&window=%s-%s" % (starttime, endtime)
        url += "&resample=%s" % resample
        if CACHE_:
            url += "&cache=0"

        res = csvparser(urllib.urlopen(url))
        if DEBUG_:
            print res
        return res[1][1:]

    def getSensorTime(self, id_, groupname, starttime, deltatime=86400, resample=10):
        endtime = starttime + deltatime
        
        url =  self.query_template % "getdata.php"
        url += "&db_group=%s" % groupname
        url += "&db_mask=%s" % id_
        url += "&window=%s-%s" % (starttime, endtime)
        url += "&resample=%s" % resample
        if CACHE_:
            url += "&cache=0"

        res = csvparser(urllib.urlopen(url))

        return map(utcsecond, res[0][1:])
                

    def getSensorTimeData(self, id_, groupname, starttime, deltatime=86400, resample=10):
        endtime = starttime + deltatime
        
        url =  self.query_template % "getdata.php"
        url += "&db_group=%s" % groupname 
        url += "&db_mask=%s" % id_
        url += "&window=%s-%s" % (starttime, endtime)
        url += "&resample=%s" % resample
        if CACHE_:
            url += "&cache=0"

        res = csvparser(urllib.urlopen(url))
        return (res[0][1:], res[1][1:])

    def getGroupStamp(self, grp):
        for a in self.groups:
            if a['name'] == grp:
                return secSinceEpoch(a['stamp'])
        return None



#    def getMaskList(self, ss, group=''):
#        pattern = "^%s.[0-9]{3}$" % ss
#        prog = re.compile(pattern)
#        pattern2 = "^%s$" % ss
#        prog2 = re.compile(pattern2)
#
#        maskstr = ""
#        for sensor in self.sensors:
#            if group:
#                if (prog.match(sensor['name']) or prog2.match(sensor['name'])) and group == sensor['group']:
#                    maskstr += ("%s" if maskstr == "" else ",%s") % sensor['id']
#            else:
#                if prog.match(sensor['name']) or prog2.match(sensor['name']):
#                    maskstr += ("%s" if maskstr == "" else ",%s") % sensor['id']
#
#        if maskstr:
#            maskList = [int(x) for x in maskstr.split(',')]
#            return maskList
#        else:
#            return None
#     
#
#    def getDataByMask(self, mask, grp, stime, deltatime=86400):
#        st = stime
#        et = stime + deltatime
#
#        maskstr = ','.join([str(n) for  n in mask])
#
#        query_str = "http://%s/adei/services/getdata.php?" % self.host
#        query_str += "db_server=%s" % self.server
#        query_str += "&db_name=%s" % self.name 
#        query_str += "&db_group=%s" % grp
#        query_str += "&db_mask=%s" % maskstr
#        query_str += "&window=%s-%s" % (st, et)
#
#        F = urllib.urlopen(query_str)
#        try:
#            csvreader = csv.reader(F, skipinitialspace=True)
#        except Exception as ex:
#            sys.stdout.write('{"error": "%s",     ' % 'unable to get ADEI data')
#            sys.stdout.write('"exception": "%s"}\n' % type(ex).__name__)
#
#        csvreader.next()
#
#        time = []
#        data = []
#        for n, row in enumerate(csvreader):
#            time.append(utcsecond(row[0]))
#            data.append(row[1:])
#        #print "%8d record read from adei" % (n+1)
#
#        data = np.array(data, dtype='f')
#        return [time, data.transpose()]


def main():
    # kitcube on local chuan
    #host = 'localhost'
    #server = 'KITCUBE'
    #name = 'HDCP2'

    # Katrin
    host = 'katrin.kit.edu/adei-detector'
    server = 'detector'
    name = 'katrin'

    hatpro = adeiReader()
    hatpro.setup(host, server, name)

    for k,v in hatpro.groups.iteritems(): 
        print k
        for kk,vv in v['sensors'].iteritems():
            print kk, vv


    #hatpro.updateSensor('name', 'column')
    #hatpro.updateSensor('axis2', 'axis2_val')
    #for i, s in enumerate(hatpro.sensors): print i, s

    #print hatpro.getMaskList('L1B.AZIMUTH.ANGLE','Data_080_RPG_nc_l1b')
    #print hatpro.getMaskList('L1B.BRIGHT.TEMP.IR', 'Data_080_RPG_nc_l2a')
    #print hatpro.getMaskList('L1B.AZIMUTH.ANGLE')
    #print

    #print hatpro.sensorFilter('L1B.BRIGHT.TEMP.IR')
    #print hatpro.sensors[16]
    #print 

    #print hatpro.sensorFilter('L2A.ATM.WAT.VAP.CNT')
    #print hatpro.sensors[20]
    #print

    #st = hatpro.groupLastDay(hatpro.sensors[20]['group'])
    #st = secSinceEpoch(st)

    #print hatpro.getSensorTime(20, st-86400, 100)
    #print hatpro.getSensorData(20, st-86400, 100)
    #print hatpro.getSensorTimeData(20, st-86400, 100)

if __name__ == "__main__":
    main()



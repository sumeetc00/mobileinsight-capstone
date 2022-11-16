import os
import sys
import time
import dis
import json
from datetime import datetime

from mobile_insight.monitor import OfflineReplayer
from mobile_insight.analyzer import LteRrcAnalyzer,WcdmaRrcAnalyzer

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from mobile_insight.analyzer.analyzer import *

class CustomizedAnalyzer(Analyzer):
    def __init__(self, f):
        Analyzer.__init__(self)
        self.add_source_callback(self.__msg_callback)
        self.f = f
    
    def set_source(self, source):
        """
        Set the trace source. Enable the cellular signaling messages
        :param source: the trace source (collector).
        """
        Analyzer.set_source(self, source)
        source.enable_log("LTE_PHY_PDSCH_Stat_Indication")

    def __msg_callback(self, msg):
        log_item = msg.data.decode()
        dt = log_item['timestamp']
        log_item['timestamp'] = time.mktime(dt.timetuple()) + dt.microsecond/1000000

        # print(log_item)
        # print("\n \n")
        self.f.write(str(log_item))
        self.f.write("\n")

class CustomizedAnalyzerBrute(Analyzer):
    def __init__(self):
        Analyzer.__init__(self)
        self.add_source_callback(self.__msg_callback)
        self.logs = []
    
    def set_source(self, source):
        """
        Set the trace source. Enable the cellular signaling messages
        :param source: the trace source (collector).
        """
        Analyzer.set_source(self, source)
        source.enable_log("LTE_PHY_PDSCH_Stat_Indication")

    def __msg_callback(self, msg):
        log_item = msg.data.decode()
        dt = log_item['timestamp']
        log_item['timestamp'] = time.mktime(dt.timetuple()) + dt.microsecond/1000000
        self.logs.append(str(log_item))


# write to file
# f = open("out.txt", "w")

# src = OfflineReplayer()
# src.set_input_path("offline_logs/")
# your_analyzer = CustomizedAnalyzer()
# your_analyzer.set_source(src) 
# src.run()
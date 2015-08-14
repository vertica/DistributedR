import re
import sys
from datetime import datetime
import pdb


class RequestTraceParser:
    requests = []
    def __init__(self):
        self.regex = r'(\d+-\w+-\d+\s+\d+:\d+:\d+\.\d+)\s+\[DEBUG\]\s+### begin(.*?)(\d+-\w+-\d+\s+\d+:\d+:\d+\.\d+)\s+\[DEBUG\]\s+### end'
        #self.regex2 = r'type\:\s+(\w+)'
        self.regex3 = r'(.*?)(\d+-\w+-\d+\s+\d+:\d+:\d+\.\d+)\s+\[DEBUG\]\s+type:\s+(\w+)(.*)'
        self.excludes = ['HELLO','HELLOREPLY']

    def parse(self, filename, excludes=[]):
        with open(filename) as f:
            c = f.read()
            tasks = re.findall(self.regex, c, re.DOTALL|re.MULTILINE)
            try:
                for i,(parsed_ts,t,_) in enumerate(tasks):
                    component,ts,reqtype,detailed_task = re.findall(self.regex3, t, re.DOTALL|re.MULTILINE)[0]
                    component = component.replace('\n','').replace(' ','')
                    if reqtype not in excludes:
                        request = {'component': component, 'timestamp':ts, 'type': reqtype, 'detailed_message': detailed_task}
                        self.requests.append(request)
            except:
                pdb.set_trace()

    def get_by_type(self, reqtype):
        return self.get_by_criteria('type', reqtype)

    def get_by_component(self, component):
        return self.get_by_criteria('component', component)

    def get_by_criteria(self, key, value):
        requests = []
        for r in self.requests:
            if r[key] == value: requests.append(r)
        return requests

    def get_all(self):
        return self.requests




if __name__ == "__main__":
    r = RequestTraceParser()
    r.parse(sys.argv[1], excludes=['HELLO', 'HELLOREPLY'])

    print '###\n### Master\n###\n'
    for request in r.get_by_component('MasterRequestLogger'):
        print request
    print '###\n### Worker\n###\n'
    for request in r.get_by_component('WorkerRequestLogger'):
        print request

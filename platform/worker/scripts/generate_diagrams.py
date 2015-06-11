import re
import pdb
import textwrap
import sys

from parse_request_trace import *

class JsSequenceDiagramGenerator:
    def __init__(self):
        self.master_ts_template = 'Note left of MASTER: %s\n'
        self.worker_ts_template = 'Note right of WORKER: %s\n'
        self.worker_template = 'MASTER->WORKER: "%s"\n'
        self.master_template = 'WORKER->MASTER: "%s"\n'

    def save_file(self,tasks):
        interactions = ''
        first_ts = None
        for t in tasks:
            if 'WorkerRequestLogger' in t['component']:
                interactions += self.master_ts_template %('%s'%(t['timestamp']))
                interactions += self.master_template %(r'%s\n%s'%(t['type'], t['detailed_message'].encode('string_escape')))
            elif 'MasterRequestLogger' in t['component']:
                interactions += self.worker_ts_template %('%s'%(t['timestamp']))
                interactions += self.worker_template %(r'%s\n%s'%(t['type'], t['detailed_message'].encode('string_escape')))
            else:
                pdb.set_trace()
                raise Exception('Unknown component')
        return interactions




class MscSequenceDiagramGenerator:
    def __init__(self):
        self.master_template = 'a=>b: [label="%s", ident="left"];\n'
        self.worker_template = 'a<=b: [label="%s", ident="left"];\n'
        self.template = '''msc {
hscale="auto";
 a [label="MASTER"], b [label="WORKER"];
 %s
}'''

    def escape_string(self, string):
        return string.encode('string_escape').replace('"', '\\"').replace('{', '\\{').replace('}', '\\}').replace('[', '\\[').replace(']', '\\]')

    def save_file(self,tasks):
        interactions = ''
        first_ts = None
        for t in tasks:
            if 'WorkerRequestLogger' in t['component']:
                interactions += self.master_template %(r'%s\n%s'%(t['type'], self.escape_string(t['detailed_message'])))
            elif 'MasterRequestLogger' in t['component']:
                interactions += self.worker_template %(r'%s\n%s'%(t['type'], self.escape_string(t['detailed_message'])))
            else:
                pdb.set_trace()
                raise Exception('Unknown component')
        return self.template%(interactions)


if __name__ == "__main__":

    r = RequestTraceParser()
    r.parse(sys.argv[1], excludes=['HELLO', 'HELLOREPLY'])

    j = JsSequenceDiagramGenerator()
    js_file_contents = j.save_file(r.get_all())
    open(sys.argv[2],'w').write(js_file_contents)

    m = MscSequenceDiagramGenerator()
    msc_file_contents = m.save_file(r.get_all())
    open(sys.argv[3],'w').write(msc_file_contents)


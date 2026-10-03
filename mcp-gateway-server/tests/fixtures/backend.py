import sys,json,os,time
initialized=False
for line in sys.stdin:
    request=json.loads(line)
    method=request['method']
    if method=='initialize':
        result={'protocolVersion':'2025-06-18','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif method=='notifications/initialized':
        initialized=True
        continue
    elif not initialized:
        print(json.dumps({'jsonrpc':'2.0','id':request['id'],'error':{'code':-32000,'message':'not initialized'}}),flush=True)
        continue
    elif method=='tools/list':
        result={'tools':[{'name':'echo','description':'Echo a value','inputSchema':{'type':'object','properties':{'value':{'type':'string'}},'required':['value']}}]}
    elif method=='tools/call':
        value=request['params']['arguments']['value']
        if value=='slow':time.sleep(10)
        result={'content':[{'type':'text','text':value}],'structuredContent':{'pid':os.getpid()}}
    else:
        result={}
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)

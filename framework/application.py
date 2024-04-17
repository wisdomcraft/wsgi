# coding=utf-8
import json

#全局变量, 用来跨模块传递数据
_global_dict = {}

#应用
class Application():
    
    
    __route     = []
    
    
    def __init__(self, root=[]):
        self.__route = root
    
    
    def __call__(self, environ, start_response):
        global _global_dict
        _global_dict['environ'] = environ
    
        header = [
            ('Access-Control-Allow-Origin', '*'),
            ('Access-Control-Allow-Methods', '*'),
            ('Access-Control-Allow-Headers', '*'),
        ]
        #通过URI找到对应的类
        uri         = environ.get('PATH_INFO', '/')
        classname   = None
        for _route in self.__route:
            if uri == _route[0]:
                classname = _route[1]
                break
        if classname == None:
            header.append( ('Content-Type', 'text/html; charset=utf-8') )
            start_response("404 NOT FOUND", header)
            return [b'404 not found']
        
        #获取METHOD并检查
        method = environ.get('REQUEST_METHOD', 'GET')
        method = method.lower()
        if method not in ['get', 'post', 'put', 'delete', 'options', 'head']:
            header.append( ('Content-Type', 'text/html; charset=utf-8') )
            start_response("404 NOT FOUND", header)
            return [b'404 not found']
        elif method in ['options', 'head']:
            header.append( ('Content-Type', 'text/html; charset=utf-8') )
            start_response("200 OK", header)
            return [b'']
        elif method not in dir(classname):
            header.append( ('Content-Type', 'text/html; charset=utf-8') )
            start_response("404 NOT FOUND", header)
            return [b'404 not found']
        
        #获取输出数据
        result = ''
        if method == 'get':
            result = classname().get()
        elif method == 'post':
            result = classname().post()
        elif method == 'put':
            result = classname().put()
        elif method == 'delete':
            result = classname().delete()
        
        content_type    = 'text/html; charset=utf-8'
        if isinstance(result, list) or isinstance(result, dict):
            result      = json.dumps(result, ensure_ascii=False)
            content_type= 'application/json; charset=utf-8'
        elif isinstance(result, int) or isinstance(result, float):
            result      = str(result)
        
        header.append( ('Content-Type', content_type) )
        start_response("200 OK", header)
        return [result.encode('utf-8')]


#=============================================================


#http的request请求数据
class Request():
    
    environ = {}
    
    def __init__(self):
        global _global_dict
        self.environ = _global_dict['environ']




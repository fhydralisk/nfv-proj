"""
Created on 2016-3-3

@author: hydra
"""

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import threading, json, traceback
import base64


class ControllerHTTPHandler(BaseHTTPRequestHandler):
    FACTORY_REDIRECTOR = None
    FACTORY_CACHE = None

    def write_common_header(self, contenttype):
        self.protocol_version = 'HTTP/1.1'
        self.send_response(200)
        self.send_header('Content-Type', contenttype)
        self.end_headers()

    def do_GET(self):
        # global fRedirector, fCache
        # self.send_error(404)
        # print 'get'
        print 'recv: GET ' + self.path
        try:
            if 'utf-8' not in self.headers['Accept-Charset'].split(','):
                self.send_error(415)
                return
        except KeyError:
            # no this field
            pass

        path_components = self.path.split('/')[1:]
        # print path_components
        try:
            if path_components[0] == 'cache':
                if path_components[1] == 'names':
                    self.get_cache_names()

                elif path_components[1] == 'resources':
                    if len(path_components) > 3:
                        res = '/'.join(path_components[3:])
                        self.get_cache_hit_count_curve(path_components[1], res)
                    else:
                        self.get_cache_resources(path_components[2])

                elif path_components[1] == 'status':
                    self.get_cache_status(path_components[2])

                else:
                    raise NameError

            elif path_components[0] == 'redirector':
                if path_components[1] == 'names':
                    self.get_redirector_names()
                elif path_components[1] == 'status':
                    self.get_redirector_status(path_components[2])
                elif path_components[1] == 'redirectTable':
                    self.get_redirect_table(path_components[2])
                elif path_components[1] == 'hitCountCurve':
                    res = '/'.join(path_components[3:])
                    self.get_redirector_hit_count_curve(path_components[2], res)
                else:
                    raise NameError

            elif path_components[0] == 'controller':
                self.send_error(501)
                return
            elif path_components[0] == 'tests':
                if path_components[1] == 'pushRedirectTable':
                    result = self.FACTORY_REDIRECTOR.push_redirect_table('redirector0', {
                        'http://www.taobao.com/': '192.168.100.32:80'})
                    self.response_json(result)
                elif path_components[1] == 'addRes':
                    result = self.FACTORY_CACHE.send_caching_command(
                      'cache0',
                      'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png',
                      'addRes'
                    )
                    self.response_json(result)
                elif path_components[1] == 'removeRes':
                    result = self.FACTORY_CACHE.send_caching_command(
                      'cache0',
                      'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png',
                      'removeRes'
                    )
                    self.response_json(result)
            elif path_components[0] == 'v1':
                if path_components[1] == 'cache_info':
                    caches = self.FACTORY_CACHE.client_dict.keys()
                    redirectors = self.FACTORY_REDIRECTOR.client_dict.keys()

                    cachecommand = [{'name': cache, 'call': 'queryStatus'} for cache in caches] + \
                                   [{'name': cache, 'call': 'queryResources'} for cache in caches]

                    redirectorcommand = [{'name': redirector, 'call': 'queryRedirectTable'} for redirector in
                                         redirectors]

                    print "getting cache result"
                    cacheresults = self.FACTORY_CACHE.syncActiveGroupMessage(cachecommand)
                    print "cache result get, getting redirector result"
                    redirectorresults = self.FACTORY_REDIRECTOR.syncActiveGroupMessage(redirectorcommand)
                    print "redirector result get"

                    cache_nodes = {}
                    redirector_nodes = {}

                    for cache in caches:
                        cache_nodes[cache] = {'ipcfg': self.FACTORY_CACHE.get_client_address(cache)}

                    for redirector in redirectors:
                        redirector_nodes[redirector] = {'ipcfg': self.FACTORY_REDIRECTOR.get_client_address(redirector)}

                    for cacheresult in cacheresults:
                        if cacheresult['call'] == 'queryStatus':
                            cache_nodes[cacheresult['name']]['status'] = cacheresult['package']['result']
                        elif cacheresult['call'] == 'queryResources':
                            cache_nodes[cacheresult['name']]['resource'] = cacheresult['package']['result']

                    for redirectorresult in redirectorresults:
                        if redirectorresult['call'] == 'queryRedirectTable':
                            redirector_nodes[redirectorresult['name']]['resources'] = redirectorresult['package'][
                                'result']

                    dictroot = {
                        'errorcode': 200, 'errorinfo': 'OK',
                        'cache_controller_id': 'controller0',
                        'cache_nodes': cache_nodes,
                        'redirector_nodes': redirector_nodes
                    }

                    self.response_json(dictroot)
                elif path_components[1] == 'cache':
                    if path_components[2][:5] == 'data=':
                        data = path_components[2][5:]
                        jsdata = json.loads(base64.b64decode(data))
                        if jsdata.has_key('name') and jsdata.has_key('url'):
                            self.get_cache_hit_count_curve(jsdata['name'], jsdata['url'], True)
                        else:
                            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

                    else:
                        raise NameError
                elif path_components[1] == 'redirector':
                    # self.responseJson(json.loads(data))
                    if path_components[2][:5] == 'data=':
                        data = path_components[2][5:]
                        jsdata = json.loads(base64.b64decode(data))
                        if jsdata.has_key('name') and jsdata.has_key('url'):
                            self.get_redirector_hit_count_curve(jsdata['name'], jsdata['url'], True)
                        else:
                            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})
                    else:
                        raise NameError
                else:
                    raise NameError
            else:
                raise NameError
        except (IndexError, NameError):
            self.send_error(404)
        except Exception, e:
            traceback.print_exc()
            self.send_error(500, str(e))

    def do_POST(self):
        print 'recv: POST ' + self.path
        try:
            postdata = self.rfile.read(int(self.headers['content-length']))
            print postdata
            post = json.loads(postdata)
            path_components = self.path.split('/')[1:]

            if path_components[0] == 'cache':

                if path_components[1] == 'resources':
                    result = self.FACTORY_CACHE.send_caching_command(path_components[2], post['url'], post['method'])
                    self.response_json(result)


                else:
                    raise NameError

            elif path_components[0] == 'redirector':

                if path_components[1] == 'redirectTable':
                    if post.has_key('flush'):
                        flush = post['flush']
                    else:
                        flush = 1

                    result = self.FACTORY_REDIRECTOR.push_redirect_table(path_components[2], post['table'], flush)
                    self.response_json(result)

                else:
                    raise NameError

            elif path_components[0] == 'controller':
                self.send_error(501)
                return
            elif path_components[0] == 'tests':
                self.send_error(501)
            else:
                raise NameError
        except (IndexError, NameError):
            self.send_error(404)
        except (KeyError, ValueError):
            # json
            self.send_error(400)
        except Exception, e:
            print e
            self.send_error(500, str(e))

    def response_json(self, jsondict):
        self.write_common_header('application/json')
        self.wfile.write(json.dumps(jsondict))

    def get_redirector_names(self):
        redirector = self.FACTORY_REDIRECTOR.client_dict.keys()
        self.response_json(redirector)

    def get_redirector_status(self, name):
        status = self.FACTORY_REDIRECTOR.query_status(name)
        if status:
            self.response_json(status)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

    def get_redirect_table(self, name):
        rd_table = self.FACTORY_REDIRECTOR.query_redirect_table(name)
        if rd_table:
            self.response_json(rd_table)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

    def get_redirector_hit_count_curve(self, name, url, simpleRequest=False):
        hitcountcurve = self.FACTORY_REDIRECTOR.query_hit_count_curve(name, url)
        if hitcountcurve:
            if simpleRequest:
                simp_response = {
                  'redirector': name,
                  'url': url,
                  'errorcode': hitcountcurve['error'],
                  'errorinfo': hitcountcurve['errordescription']
                }
                if hitcountcurve['error'] == 200:
                    simp_response['hitcount'] = hitcountcurve['result']
                self.response_json(simp_response)
            else:
                self.response_json(hitcountcurve)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

    def get_cache_names(self):
        caches = self.FACTORY_CACHE.client_dict.keys()
        self.response_json(caches)

    def get_cache_status(self, name):
        status = self.FACTORY_CACHE.query_status(name)
        if status:
            self.response_json(status)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

    def get_cache_resources(self, name):
        resources = self.FACTORY_CACHE.query_resources(name)
        if resources:
            self.response_json(resources)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})

    def get_cache_hit_count_curve(self, name, url, simpleRequest=False):
        hitcountcurve = self.FACTORY_CACHE.query_hit_count_curve(name, url)
        if hitcountcurve:
            if simpleRequest:
                simp_response = {
                  'cache': name,
                  'url': url,
                  'errorcode': hitcountcurve['error'],
                  'errorinfo': hitcountcurve['errordescription']
                }
                if hitcountcurve['error'] == 200:
                    simp_response['hitcount'] = hitcountcurve['result']

                self.response_json(simp_response)
            else:
                self.response_json(hitcountcurve)
        else:
            self.response_json({'errorcode': 404, 'errorinfo': 'Not found'})


def server_deamon(port):
    http_server = HTTPServer(('', int(port)), ControllerHTTPHandler)
    print 'http server starting...'
    http_server.serve_forever()


def start_server(port, fr, fc):
    ControllerHTTPHandler.FACTORY_CACHE = fc
    ControllerHTTPHandler.FACTORY_REDIRECTOR = fr
    t = threading.Thread(target=server_deamon, args=(port,))
    t.setDaemon(True)
    t.start()


if __name__ == '__main__':
    pass

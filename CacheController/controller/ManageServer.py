"""
Created on 2016

@author: hydra
"""""

from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor, task

import threading
import json
from twisted.internet.address import IPv4Address
import httplib2


class HyFactoryBase(Factory):
    dict_lock = threading.RLock()
    '''
    client_dict: { 
                  client_name : {pobj:protocol , hearbeat:hb, ...} , ...}
    '''
    field_hb = 'heartbeat'
    field_protocol = 'pobj'

    def __init__(self, timeout_dead=60, timeout_kill=120, timeout_query=10, server=None):
        # self.timer = threading.Timer(timer_interval, self.timerHit, [self])
        self.timeout_dead = timeout_dead
        self.timeout_kill = timeout_kill
        self.timeout_query = timeout_query
        self.time = 0
        self.server = server
        # self.timer.run()

        self.timer = task.LoopingCall(self.timer_hit)
        self.timer.start(1, False)
        self.client_dict = {}
        self.command_id = 0
        '''
        queries:
        {
          command_id : {event : qevent_object, timer : timer},...
        }
        '''
        self.queries = {}

    def get_protocol(self, name):
        return self.client_dict[name][HyFactoryBase.field_protocol]

    def timer_hit(self):

        self.time += 1
        self.dict_lock.acquire()

        for k, v in self.client_dict.items():
            v[self.field_hb] += 1
            if v[self.field_hb] > self.timeout_kill:
                # timeout kill
                if v[self.field_protocol].connected:
                    v[self.field_protocol].transport.abortConnection()

                del self.client_dict[k]

        for k, v in self.queries.items():
            v['timer'] += 1
            if v['timer'] > self.timeout_query:
                self.set_result_event(k, False)

        self.dict_lock.release()

    def time_to_call(self, interval):
        return self.time % interval == 0

    def get_command_id(self):
        command_id = self.command_id
        self.command_id += 1
        return command_id

    def add_client_node(self, node):
        self.dict_lock.acquire()

        if node.name not in self.client_dict:
            self.client_dict[node.name] = {}

        self.client_dict[node.name][self.field_protocol] = node
        self.client_dict[node.name][self.field_hb] = 0

        print self.client_dict
        self.dict_lock.release()

    def remove_client_node(self, node):
        try:
            if self.client_dict[node.name][self.field_protocol] is node:
                del self.client_dict[node.name]
        except KeyError:
            pass

    def heartbeat(self, node):
        self.dict_lock.acquire()
        self.client_dict[node.name][self.field_hb] = 0
        self.dict_lock.release()

    def append_client_field(self, node, key, value):
        self.dict_lock.acquire()
        try:
            self.client_dict[node.name][key] = value
        except KeyError:
            pass

        self.dict_lock.release()

    def stopFactory(self):
        self.timer.stop()
        Factory.stopFactory(self)

    def add_result_event(self, command_id, qevent):
        self.queries[command_id] = {'event': qevent, 'timer': 0}

    def set_result_event(self, command_id, result=False, package=None):
        if self.queries.has_key(command_id):
            self.queries[command_id]['event'].result = result
            if package:
                self.queries[command_id]['event'].package = package
            self.queries[command_id]['event'].set()
            del self.queries[command_id]

    def sync_active_message(self, name, call, *args, **kwargs):
        if name not in self.client_dict.keys():
            raise NameError

        qevent = threading.Event()
        qevent.clear()
        qevent.result = False

        node = self.client_dict[name][HyFactoryBase.field_protocol]
        kwargs['qevent'] = qevent
        kwargs['wait'] = True
        getattr(node, call)(*args, **kwargs)
        if qevent.result:
            return qevent.package

        return None

    def syncActiveGroupMessage(self, commandlist):
        qevents = []
        for unit in commandlist:
            name = unit['name']
            if name not in self.client_dict.keys():
                continue

            call = unit['call']

            qevent = threading.Event()
            qevent.clear()
            qevent.result = False
            qevents.append((name, call, qevent))

            node = self.client_dict[name][HyFactoryBase.field_protocol]

            try:
                args = unit['args']
            except KeyError:
                args = []

            try:
                kwargs = unit['kwargs']
            except KeyError:
                kwargs = {}

            kwargs['qevent'] = qevent
            kwargs['wait'] = False
            getattr(node, call)(*args, **kwargs)

        ret_val = []
        # print self.queries
        # print threading.current_thread()
        for name, call, qevent in qevents:
            qevent.wait()
            if qevent.result:
                ret_val.append({'name': name, 'call': call, 'package': qevent.package})

        return ret_val

    def get_client_address(self, name, internal_addr=True):
        if internal_addr:
            internal_addr = self.client_dict[name][HyFactoryBase.field_protocol].internal_address
            return {'ip': internal_addr.host, 'port': internal_addr.port}
        else:
            return self.client_dict[name][HyFactoryBase.field_protocol].get_address()


class HyProtocolBase(Protocol):
    def __init__(self):
        self.name = None
        self.datapool = ''
        self.package_received = False
        self.internal_address = None

    def connectionLost(self, reason):
        try:
            self.factory.remove_client_node(self)
        except:
            print 'duplicate client?'

        Protocol.connectionLost(self, reason=reason)

    def connectionMade(self):
        Protocol.connectionMade(self)

    def dataReceived(self, data):

        # print threading.current_thread()
        self.datapool = self.datapool + data

        packages = self.proceed_data()

        for package in packages:
            try:
                package_type = package['type']
            except Exception, e:
                print e
                print "err: type excepted"
                continue

            if not self.package_received:
                self.package_received = True

                if package_type != 'name':
                    self.transport.abortConnection()
                    print 'first packet type error'
                    return
                try:
                    self.first_package(package)

                except KeyError:
                    print 'first package parse failed'
                    self.transport.abortConnection()
                    return
            else:
                if package_type == self.factory.field_hb:
                    # TODO: should check if the object is me

                    # clear timeout due to heartbeat

                    self.factory.heartbeat(self)
                    if 'event' in package:
                        self.event_received(package['event'])

                else:
                    # deliver package here to subclasses
                    if package_type in ('queryresult', 'commandresult'):
                        # hit events here
                        self.factory.set_result_event(package['commandid'], True, package)

                    self.packet_received(package)
                pass

        Protocol.dataReceived(self, data)

    def packet_received(self, package):
        pass

    def event_received(self, r_event):
        pass

    def first_package(self, package):
        self.name = package['name']
        self.internal_address = IPv4Address(host=package['internalAddress'][0], port=package['internalAddress'][1],
                                            type='TCP')
        self.factory.add_client_node(self)
        if package['deviceType'] != self.factory.device_type:
            print 'device type error:' + str(self.transport.getPeer())
            print 'expect ' + self.factory.device_type + ', but ' + package['deviceType']
            self.transport.abortConnection()

    def send_message(self, msg_dict):
        try:
            # TODO: make it thread safe.
            self.transport.write(json.dumps(msg_dict) + '\r\n')

        except TypeError:
            print 'error: message dict error'

    def send_command(self, command, message=None, q_event=None, wait=False):
        return self.send_active_message('command', command, message, q_event, wait)

    def send_query(self, query, message=None, q_event=None, wait=False):

        return self.send_active_message('query', query, message, q_event, wait)

    def send_active_message(self, pkgtype, cmd, message, q_event, wait):
        package = {'type': pkgtype, pkgtype: cmd, 'commandid': self.factory.get_command_id()}

        if message:
            for k, v in message.items():
                package[k] = v

        if q_event:
            self.factory.add_result_event(package['commandid'], q_event)

        self.send_message(package)

        if wait:
            if q_event is None:
                raise ValueError

            q_event.wait()

            return q_event.result

        return None

    def proceed_data(self):

        if len(self.datapool) <= 2:
            return []

        messages = self.datapool.split('\r\n')
        msg_count = len(messages)

        # proceed tail message (maybe empty)
        self.datapool = messages[msg_count - 1]
        del messages[msg_count - 1]

        packages = []

        for message in messages:
            try:
                jo = json.loads(message)
                packages.append(jo)
            except:
                print 'message parse failed'

        return packages

    def get_address(self):
        addr = self.transport.getPeer()
        return {'ip': addr.host, 'port': addr.port}


class RedirectorProtocol(HyProtocolBase):
    def __init__(self):
        HyProtocolBase.__init__(self)

    def packet_received(self, package):
        pkgtype = package['type']
        if pkgtype == 'queryresult':
            try:
                querytype = package['reply']
            except KeyError:
                print 'received an answer packet without "reply" key.'
                return

            if querytype == 'status':
                self.factory.append_client_field(self, 'status', package['result'])

    def update_status(self, status):
        # self.factory.client_dict[self.name]['status'] = status
        self.factory.append_client_field(self, 'status', status)

    def query_status(self, q_event=None, wait=False):
        return self.send_query('status', None, q_event, wait)

    def push_redirect_table(self, tbl, flush=1, q_event=None, wait=False):
        return self.send_command('updateRedirectTable', {'table': tbl, 'flush': flush}, q_event, wait)

    def query_redirect_table(self, q_event=None, wait=False):
        return self.send_query('redirectTable', None, q_event, wait)

    def query_hit_count_curve(self, url, q_event=None, wait=False):
        return self.send_query('hitcountcurve', {'res': url}, q_event, wait)


class RedirectorFactory(HyFactoryBase):
    protocol = RedirectorProtocol
    device_type = 'redirector'

    def __init__(self, timeout_dead=60, timeout_kill=120, timeout_query=10, server=None):
        HyFactoryBase.__init__(self, timeout_dead, timeout_kill, timeout_query, server)

    def query_status(self, name):
        return self.sync_active_message(name, 'queryStatus')

    def query_redirect_table(self, name):
        return self.sync_active_message(name, 'queryRedirectTable')

    def query_hit_count_curve(self, name, url):
        return self.sync_active_message(name, 'queryHitCountCurve', url)

    def push_redirect_table(self, name, tbl, flush=1):
        return self.sync_active_message(name, 'pushRedirectTable', tbl, flush)


class CacheProtocol(HyProtocolBase):
    def __init__(self):
        self.overload = False
        self.caching_port = None
        self.time_granularity = None
        self.time_hold = None
        self.timestamp_ref = None
        HyProtocolBase.__init__(self)

    def first_package(self, package):
        HyProtocolBase.first_package(self, package)
        self.caching_port = package['cachingPort']
        self.time_granularity = package['timeopt'][0]
        self.time_hold = package['timeopt'][1]
        self.timestamp_ref = package['timeopt'][2]

    def packet_received(self, package):
        pkg_type = package['type']
        if pkg_type == 'queryresult':
            try:
                query_type = package['reply']
            except KeyError:
                print 'received an answer packet without "reply" key.'
                return

            if query_type == 'status':
                self.factory.append_client_field(self, 'status', package['result'])

    def event_received(self, r_event):
        if r_event == 'overload':
            # overload event processing
            print "Event: overload. cache: %s" % self.name
            self.overload = True

        HyProtocolBase.event_received(self, r_event)

    def send_caching_command(self, url, command='addRes', qevent=None, wait=None):
        self.send_command(command, {'res': url}, qevent, wait)

        # TODO: how to 'return' the message? threading consideration should be taken...

    def query_status(self, q_event=None, wait=False):
        self.send_query('status', None, q_event, wait)

    def query_resources(self, q_event=None, wait=False):
        self.send_query('resources', None, q_event, wait)

    def query_hit_count_curve(self, url, q_event=None, wait=False):
        self.send_query('hitcountcurve', {'res': url}, q_event, wait)

    def query_hit_count_curves(self, q_event=None, wait=False):
        self.send_query('hitcountcurves', None, q_event, wait)


class CacheFactory(HyFactoryBase):
    protocol = CacheProtocol
    interval_check_status = 30
    report_thres = 0.5
    device_type = 'cache'
    curve_count_sum_min = 10

    def __init__(self, timeout_dead=60, timeout_kill=120, timeout_query=10, server=None):
        HyFactoryBase.__init__(self, timeout_dead, timeout_kill, timeout_query, server)

    def query_status(self, name):
        return self.sync_active_message(name, 'queryStatus')

    def query_resources(self, name):
        return self.sync_active_message(name, 'queryResources')

    def query_hit_count_curve(self, name, url):
        return self.sync_active_message(name, 'queryHitCountCurve', url)

    def send_caching_command(self, name, url, command):
        return self.sync_active_message(name, 'sendCachingCommand', url=url, command=command)

    def query_resources_of_all(self):
        name2query = [{'name': name, 'call': 'queryResources'} for name in self.client_dict.keys()]
        result = self.syncActiveGroupMessage(name2query)
        return result

    def timer_hit(self):
        if self.time_to_call(self.interval_check_status):
            checking_thread = threading.Thread(target=self.check_status)
            checking_thread.setDaemon(True)
            checking_thread.start()

        HyFactoryBase.timer_hit(self)

    def check_status(self):
        if len(self.client_dict.keys()) > 0:
            overloads = [cache[self.field_protocol] for cache in self.client_dict.values() if
                         cache[self.field_protocol].overload]
            if float(len(overloads)) / float(len(self.client_dict.keys())) > self.report_thres:
                self.server.report_to_coordinator(self, 'overload')

            hccs_query = [{'name': cache.name, 'call': 'queryHitCountCurves'} for cache in overloads]
            if hccs_query:
                hccs = self.syncActiveGroupMessage(hccs_query)

                # call server to handle and proceed the overload event
                self.server.proceed_cache_overload(hccs)

            for overload in overloads:
                # clear flags
                overload.overload = False


class ControlServer(object):
    ports = (6058, 6059)
    ip = ''

    detach_thread = None
    ControllerEndpointCache = None
    ControllerEndpointRedirector = None
    cache_curve_count_to_learn_in_min = 10

    def __init__(self, ip='', ports=(6058, 6059)):
        """
        @param ip: ip to bind and listen
        @param ports:a tuple of port to listen, first of which is cache's port and second is redirector's
        """

        self.ports = ports
        self.ip = ip
        self.cacheFactory = CacheFactory(server=self)
        self.redirectorFactory = RedirectorFactory(server=self)
        self.profile = None
        # TODO: FIXME: This shall be moved !
        self.rest_uri = "/restconf/operations/event:push-event/"
        self.rest_addr = '166.111.65.110'
        self.rest_port = 8181
        self.rest_user = 'admin'
        self.rest_pwd = 'admin'

        pass

    def run(self):
        if 'cache_curve_count_to_learn_in_min' in self.profile:
            self.cache_curve_count_to_learn_in_min = self.profile['cache_curve_count_to_learn_in_min']

        if 'uri_client_coordinator_rest' in self.profile:
            self.rest_uri = self.profile['uri_client_coordinator_rest']

        if 'username_client_coordinator_rest' in self.profile:
            self.rest_user = self.profile['username_client_coordinator_rest']

        if 'password_client_coordinator_rest' in self.profile:
            self.rest_pwd = self.profile['password_client_coordinator_rest']

        self.rest_addr = self.profile['addr_client_coordinator']
        self.rest_port = self.profile['port_client_coordinator']

        print 'trying to start manage server...'

        if self.ControllerEndpointCache is not None or self.ControllerEndpointRedirector is not None:
            print "err: trying to rerun controller manage server"
            return

        try:
            self.ControllerEndpointCache = TCP4ServerEndpoint(reactor=reactor, port=self.ports[0], interface=self.ip)
            self.ControllerEndpointRedirector = TCP4ServerEndpoint(reactor=reactor, port=self.ports[1],
                                                                   interface=self.ip)
            self.ControllerEndpointCache.listen(self.cacheFactory)

            print 'cache server start listening'
            self.ControllerEndpointRedirector.listen(self.redirectorFactory)
            print 'redirector server start listening'
        except:
            print "err: listen() failed, check port usage"

        reactor.run()

        print 'exiting manage server...'

        self.detach_thread = None

    def run_async(self):
        if self.detach_thread is not None:
            print "err: trying to rerun controller manage server"
            return

        self.detach_thread = threading.Thread(target=self.run)
        self.detach_thread.setDaemon(True)
        self.detach_thread.start()

    def stop(self):
        if self.detach_thread is None:
            return

        print 'trying to stop server'

        reactor.stop()

        self.detach_thread.join()
        self.detach_thread = None
        self.ControllerEndpointCache = None
        self.ControllerEndpointRedirector = None

        print 'server terminated'

    def recalc_redirect_table(self):
        pass

    def report_to_coordinator(self, caller, event):
        if event == 'overload':
            # TODO: this shall be moved to conf
            event_to_report = '''
{
    "input":{
        "name":"event1";
        "event-type":"cache",
        "event-detail":[
            {
                "name":"detail1",
                "description":"NEW"
            }]
    }
}'''
            try:

                httplib2.debuglevel = 1
                h = httplib2.Http()
                h.add_credentials(self.rest_user, self.rest_pwd)
                response, content = h.request('http://%s:%s%s' % (self.rest_addr, self.rest_port, self.rest_uri), 'POST',
                                              event_to_report,
                                              headers={'Content-Type': 'application/json', 'Accept': '*/*'})
                print "reporting to coordinator"
                print response
                print content

            except:
                print 'connection to coordinator failed'

    def proceed_cache_overload(self, hccs):
        # recal redirect table here
        caches = self.cacheFactory.client_dict.keys()
        overload_caches = [n['name'] for n in hccs]

        normal_caches = [c for c in caches if c not in overload_caches]

        if normal_caches:
            # calc a list of max hits of each overloaded cache
            the_list = []

            for hcc in hccs:
                name = hcc['name']
                payload = hcc['package']
                max_hits = 0
                max_record = None
                curve_count_to_sum = self.get_curve_count_to_sum(name)
                for res, hit_count_curve in payload['result'].items():
                    hit_count = 0
                    if not res.startswith('http://'):
                        print 'ignoring %s' % res
                        continue

                    for count in hit_count_curve[-curve_count_to_sum:]:
                        hit_count += count['count']

                    if hit_count > max_hits:
                        max_record = (name, res, hit_count)  # node of the list
                        max_hits = hit_count
                        pass
                    pass

                if max_record:
                    print "find a max record of %s:%s" % (name, max_record)
                    the_list.append(max_record)

            the_list.sort(key=lambda k: k[2], reverse=True)
            res2move = the_list[:len(normal_caches)]
            print the_list
            # TODO: move res here. the algorithm must be modified here. but this is just a demo
            tbls_to_push = {}
            for i in range(len(res2move)):
                cache_move2 = self.cacheFactory.get_protocol(normal_caches[i])
                addr_of_normal_cache = cache_move2.internal_address

                # TODO: cache port default 80, just a demo, should be fix!
                # TODO: this is a trick, using internal address.. how to solve it?

                # res2move[i]: node; node[1]:res; res[1]:url
                tbls_to_push[res2move[i][1]] = addr_of_normal_cache.host + ':' + str(cache_move2.caching_port)

            print "trying to push redirect table to all redirectors:"
            print tbls_to_push
            # push the redirect table to each of redirectors. demo
            for redirector in self.redirectorFactory.client_dict.keys():
                self.redirectorFactory.client_dict[redirector][CacheFactory.field_protocol].push_redirect_table(
                    tbls_to_push, 0)

    def get_curve_count_to_sum(self, cache_name):
        cache_node = self.cacheFactory.get_protocol(cache_name)
        granularity = cache_node.time_granularity
        return self.cache_curve_count_to_learn_in_min / granularity + 1

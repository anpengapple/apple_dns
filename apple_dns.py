#!/usr/bin/env python
# coding: utf-8

import ConfigParser
import os
import re
import SocketServer

import dnslib
import gevent
from gevent import monkey

monkey.patch_all()
from gevent.queue import Queue
import pylru


def query(qname):
    """
    这里用文本，只是临时演示用，没有任何性能及安全方面的考虑。改进可以考虑：
    1、在开启服务器时将内容全部加载到内存，这样可以去掉LRUCache；
    2、使用redis或mysql之类的数据库；
    3、注意数据的验证，例如判断ip的正则，域名的内容等等。
    """
    with open('db.csv') as fdb:
        soa_line = fdb.readline().rstrip().split(',')
        soa = tuple(soa_line) if len(soa_line) == 2 else None
        dns = [tuple(line.rstrip('\r\n').split(',')) for line in fdb.readlines()]

    def get_answer(q, d, names):
        name = d.get(q)
        if name:
            names.append((q, name))
            get_answer(name, d, names)

    ret = []
    get_answer(qname, dict(dns), ret)
    print ret
    return ret, soa


def pack_dns(dns, answers, soa=None):
    content_type = lambda x: 'A' if re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', x) else 'CNAME'
    if answers:
        for ans in answers:
            if content_type(ans[1]) == 'A':
                dns.add_answer(dnslib.RR(ans[0], dnslib.QTYPE.A, rdata=dnslib.A(ans[1])))
            elif content_type(ans[1]) == 'CNAME':
                dns.add_answer(dnslib.RR(ans[0], dnslib.QTYPE.CNAME, rdata=dnslib.CNAME(ans[1])))
    elif soa:
        soa_content = soa[1].split()
        dns.add_auth(dnslib.RR(soa[0], dnslib.QTYPE.SOA,
                               rdata=dnslib.SOA(soa_content[0], soa_content[1], (int(i) for i in soa_content[2:]))))

    return dns


def handler(data, addr, sock):
    try:
        dns = dnslib.DNSRecord.parse(data)
    except Exception as e:
        print 'Not a DNS packet.\n', e
    else:
        dns.header.set_qr(dnslib.QR.RESPONSE)
        # 获得请求域名
        qname = dns.q.qname
        # 在LRUCache中查找缓存过域名的DNS应答包
        response = DNSServer.dns_cache.get(qname)
        print 'qname =', qname, 'response =', response

        if response:
            # 若应答已在缓存中，直接替换id后返回给用户
            response[:2] = data[:2]
            sock.sendto(response, addr)
        else:
            # 若应答不在缓存中，从db中查询（这里只用了一个简单的文件供演示）
            answers, soa = query(str(qname).rstrip('.'))
            answer_dns = pack_dns(dns, answers, soa)

            # 将查询到的应答包放入LRUCache以后使用
            DNSServer.dns_cache[qname] = answer_dns.pack()
            # 返回
            sock.sendto(answer_dns.pack(), addr)


def _init_cache_queue():
    while True:
        data, addr, sock = DNSServer.deq_cache.get()
        gevent.spawn(handler, data, addr, sock)


class DNSHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # 若缓存队列没有存满，把接收到的包放进缓存队列中（存满则直接丢弃包）
        if not DNSServer.deq_cache.full():
            # 缓存队列保存元组：(请求包，请求地址，sock)
            DNSServer.deq_cache.put((self.request[0], self.client_address, self.request[1]))


class DNSServer(object):
    @staticmethod
    def start():
        # 缓存队列，收到的请求都先放在这里，然后从这里拿数据处理
        DNSServer.deq_cache = Queue(maxsize=deq_size) if deq_size > 0 else Queue()
        # LRU Cache，使用近期最少使用覆盖原则
        DNSServer.dns_cache = pylru.lrucache(lru_size)

        # 启动协程，循环处理缓存队列
        gevent.spawn(_init_cache_queue)

        # 启动DNS服务器
        print 'Start DNS server at %s:%d\n' % (ip, port)
        dns_server = SocketServer.UDPServer((ip, port), DNSHandler)
        dns_server.serve_forever()


def load_config(filename):
    with open(filename, 'r') as fc:
        cfg = ConfigParser.ConfigParser()
        cfg.readfp(fc)

    return dict(cfg.items('DEFAULT'))


if __name__ == '__main__':
    # 读取配置文件
    config_file = os.path.basename(__file__).split('.')[0] + '.ini'
    config_dict = load_config(config_file)

    ip, port = config_dict['ip'], int(config_dict['port'])
    deq_size, lru_size = int(config_dict['deq_size']), int(config_dict['lru_size'])
    db = config_dict['db']

    # 启动服务器
    DNSServer.start()

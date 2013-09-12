import os
import redis
import connection
import socket
import time

from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.crawler import Crawler
from scrapy.settings import CrawlerSettings
from unittest import TestCase

from .dupefilter import RFPDupeFilter
from .queue import SpiderQueue, SpiderPriorityQueue, SpiderStack
from .scheduler import Scheduler
from .stats import RedisStatsCollector


# allow test settings from environment
REDIS_HOST = os.environ.get('REDIST_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))


class DupeFilterTest(TestCase):

    def setUp(self):
        self.server = redis.Redis(REDIS_HOST, REDIS_PORT)
        self.key = 'scrapy_redis:tests:dupefilter:'
        self.df = RFPDupeFilter(self.server, self.key)

    def tearDown(self):
        self.server.delete(self.key)

    def test_dupe_filter(self):
        req = Request('http://example.com')

        self.assertFalse(self.df.request_seen(req))
        self.assertTrue(self.df.request_seen(req))

        self.df.close('nothing')


class QueueTestMixin(object):

    queue_cls = None

    def setUp(self):
        self.spider = BaseSpider('myspider')
        self.key = 'scrapy_redis:tests:%s:queue' % self.spider.name
        self.server = redis.Redis(REDIS_HOST, REDIS_PORT)
        self.q = self.queue_cls(self.server, BaseSpider('myspider'), self.key)

    def tearDown(self):
        self.server.delete(self.key)

    def test_clear(self):
        self.assertEqual(len(self.q), 0)

        for i in range(10):
            # XXX: can't use same url for all requests as SpiderPriorityQueue
            # uses redis' set implemention and we will end with only one
            # request in the set and thus failing the test. It should be noted
            # that when using SpiderPriorityQueue it acts as a request
            # duplication filter whenever the serielized requests are the same.
            # This might be unwanted on repetitive requests to the same page
            # even with dont_filter=True flag.
            req = Request('http://example.com/?page=%s' % i)
            self.q.push(req)
        self.assertEqual(len(self.q), 10)

        self.q.clear()
        self.assertEqual(len(self.q), 0)


class SpiderQueueTest(QueueTestMixin, TestCase):

    queue_cls = SpiderQueue

    def test_queue(self):
        req1 = Request('http://example.com/page1')
        req2 = Request('http://example.com/page2')

        self.q.push(req1)
        self.q.push(req2)

        out1 = self.q.pop()
        out2 = self.q.pop()

        self.assertEqual(out1.url, req1.url)
        self.assertEqual(out2.url, req2.url)


class SpiderPriorityQueueTest(QueueTestMixin, TestCase):

    queue_cls = SpiderPriorityQueue

    def test_queue(self):
        req1 = Request('http://example.com/page1', priority=100)
        req2 = Request('http://example.com/page2', priority=50)
        req3 = Request('http://example.com/page2', priority=200)

        self.q.push(req1)
        self.q.push(req2)
        self.q.push(req3)

        out1 = self.q.pop()
        out2 = self.q.pop()
        out3 = self.q.pop()

        self.assertEqual(out1.url, req3.url)
        self.assertEqual(out2.url, req1.url)
        self.assertEqual(out3.url, req2.url)


class SpiderStackTest(QueueTestMixin, TestCase):

    queue_cls = SpiderStack

    def test_queue(self):
        req1 = Request('http://example.com/page1')
        req2 = Request('http://example.com/page2')

        self.q.push(req1)
        self.q.push(req2)

        out1 = self.q.pop()
        out2 = self.q.pop()

        self.assertEqual(out1.url, req2.url)
        self.assertEqual(out2.url, req1.url)


class SchedulerTest(TestCase):

    def setUp(self):
        self.server = redis.Redis(REDIS_HOST, REDIS_PORT)
        self.key_prefix = 'scrapy_redis:tests:'
        self.queue_key = self.key_prefix + '%(spider)s:requests'
        self.dupefilter_key = self.key_prefix + '%(spider)s:dupefilter'
        self.idle_before_close = 0
        self.scheduler = Scheduler(self.server, False, self.queue_key,
                                   SpiderQueue, self.dupefilter_key,
                                   self.idle_before_close)

    def tearDown(self):
        for key in self.server.keys(self.key_prefix):
            self.server.delete(key)

    def test_scheduler(self):
        # default no persist
        self.assertFalse(self.scheduler.persist)

        spider = BaseSpider('myspider')
        self.scheduler.open(spider)
        self.assertEqual(len(self.scheduler), 0)

        req = Request('http://example.com')
        self.scheduler.enqueue_request(req)
        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 1)

        # dupefilter in action
        self.scheduler.enqueue_request(req)
        self.assertEqual(len(self.scheduler), 1)

        out = self.scheduler.next_request()
        self.assertEqual(out.url, req.url)

        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        self.scheduler.close('finish')

    def test_scheduler_persistent(self):
        messages = []
        spider = BaseSpider('myspider')
        spider.log = lambda *args, **kwargs: messages.append([args, kwargs])

        self.scheduler.persist = True
        self.scheduler.open(spider)

        self.assertEqual(messages, [])

        self.scheduler.enqueue_request(Request('http://example.com/page1'))
        self.scheduler.enqueue_request(Request('http://example.com/page2'))

        self.assertTrue(self.scheduler.has_pending_requests())
        self.scheduler.close('finish')

        self.scheduler.open(spider)
        self.assertEqual(messages, [
            [('Resuming crawl (2 requests scheduled)',), {}],
        ])
        self.assertEqual(len(self.scheduler), 2)

        self.scheduler.persist = False
        self.scheduler.close('finish')

        self.assertEqual(len(self.scheduler), 0)


class ConnectionTest(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # We can get a connection from just REDIS_URL.
    def test_redis_url(self):
        settings = dict(
            REDIS_URL = 'redis://foo:bar@localhost:9001/42'
        )

        server = connection.from_settings(settings)
        connect_args = server.connection_pool.connection_kwargs

        self.assertEqual(connect_args['host'], 'localhost')
        self.assertEqual(connect_args['port'], 9001)
        self.assertEqual(connect_args['password'], 'bar')
        self.assertEqual(connect_args['db'], 42)

    # We can get a connection from REDIS_HOST/REDIS_PORT.
    def test_redis_host_port(self):
        settings = dict(
            REDIS_HOST = 'localhost',
            REDIS_PORT = 9001
        )

        server = connection.from_settings(settings)
        connect_args = server.connection_pool.connection_kwargs

        self.assertEqual(connect_args['host'], 'localhost')
        self.assertEqual(connect_args['port'], 9001)

    # REDIS_URL takes precedence over REDIS_HOST/REDIS_PORT.
    def test_redis_url_precedence(self):
        settings = dict(
            REDIS_HOST = 'baz',
            REDIS_PORT = 1337,
            REDIS_URL = 'redis://foo:bar@localhost:9001/42'
        )

        server = connection.from_settings(settings)
        connect_args = server.connection_pool.connection_kwargs

        self.assertEqual(connect_args['host'], 'localhost')
        self.assertEqual(connect_args['port'], 9001)
        self.assertEqual(connect_args['password'], 'bar')
        self.assertEqual(connect_args['db'], 42)

    # We fallback to REDIS_HOST/REDIS_PORT if REDIS_URL is None.
    def test_redis_host_port_fallback(self):
        settings = dict(
            REDIS_HOST = 'baz',
            REDIS_PORT = 1337,
            REDIS_URL = None
        )

        server = connection.from_settings(settings)
        connect_args = server.connection_pool.connection_kwargs

        self.assertEqual(connect_args['host'], 'baz')
        self.assertEqual(connect_args['port'], 1337)

    # We use default values for REDIS_HOST/REDIS_PORT.
    def test_redis_default(self):
        settings = dict()

        server = connection.from_settings(settings)
        connect_args = server.connection_pool.connection_kwargs

        self.assertEqual(connect_args['host'], 'localhost')
        self.assertEqual(connect_args['port'], 6379)


class StatsCollectorTest(TestCase):

    def setUp(self):
        self.server = redis.Redis(REDIS_HOST, REDIS_PORT)
        self.spider = BaseSpider('scrapy_redis_test')

    def tearDown(self):
        for key in self.server.keys('%s:*' % self.spider.name):
            self.server.delete(key)

    def create_collector(self, settings={}):
        crawler = Crawler(CrawlerSettings())
        for key, value in settings.items():
            crawler.settings.overrides[key] = value

        crawler.configure()

        return RedisStatsCollector(crawler)

    def run_scenario(self, settings={}):
        stats = self.create_collector(settings)
        stats.open_spider(self.spider)
        stats.set_value('foo', 'bar')
        stats.close_spider(self.spider, 'Closing.')

    # If a stat is set, it should be present in Redis.
    def test_stats_set_get(self):
        self.run_scenario()

        keys = self.server.keys('%s:stats:*' % self.spider.name)
        self.assertEqual(len(keys), 1)

        key = keys[0]
        redis_type = self.server.type(key)
        self.assertEqual(redis_type, 'hash')

        value = self.server.hget(key, 'foo')
        self.assertEqual(value, 'bar')

        hostname = self.server.hget(key, 'hostname')
        self.assertEqual(hostname, None)

    # If STATS_KEY_PATTERN is specified, that key should be set in Redis.
    def test_stats_key_pattern(self):
        settings = dict(STATS_KEY_PATTERN = '{spider}:baz:{id}')
        self.run_scenario(settings)

        keys = self.server.keys('%s:baz:*' % self.spider.name)
        self.assertEqual(len(keys), 1)

    # If STATS_KEY_PATTERN contains {hostname}, it should be replaced with the machine's hostname.
    def test_stats_key_pattern_hostname(self):
        settings = dict(STATS_KEY_PATTERN = '{spider}:{hostname}:{id}')
        self.run_scenario(settings)

        hostname = socket.gethostname()
        keys = self.server.keys('%s:%s:*' % (self.spider.name, hostname))
        self.assertEqual(len(keys), 1)

    # If STATS_KEY_EXPIRE is set, that key should expire.
    def test_stats_key_expire(self):
        settings = dict(STATS_KEY_EXPIRE = 1)
        self.run_scenario(settings)

        time.sleep(settings['STATS_KEY_EXPIRE'])
        keys = self.server.keys('%s:stats:*' % self.spider.name)
        self.assertEqual(len(keys), 0)

    # If STATS_SET_HOSTNAME is specified, the hostname should be present in Redis.
    def test_stats_set_hostname(self):
        settings = dict(STATS_SET_HOSTNAME = True)
        self.run_scenario(settings)

        keys = self.server.keys('%s:stats:*' % self.spider.name)
        self.assertEqual(len(keys), 1)

        key = keys[0]
        hostname = self.server.hget(key, 'hostname')
        self.assertTrue(hostname)
        self.assertTrue(len(hostname) > 0)

    # If multiple spiders are run under the same crawler, all stats should be saved.
    def test_stats_multiple_spiders(self):
        crawler = Crawler(CrawlerSettings())
        crawler.configure()

        s1 = RedisStatsCollector(crawler)
        s2 = RedisStatsCollector(crawler)

        s1.open_spider(self.spider)
        s2.open_spider(self.spider)
        s1.set_value('foo1', 'bar1')
        s2.set_value('foo2', 'bar2')
        s1.close_spider(self.spider, 'Closing.')
        s2.close_spider(self.spider, 'Closing.')

        keys = self.server.keys('%s:stats:*' % self.spider.name)
        self.assertEqual(len(keys), 2)

        all_values = {}
        for key in keys:
            values = self.server.hgetall(key)
            all_values = dict(all_values.items() + values.items())

        self.assertEqual(all_values['foo1'], 'bar1')
        self.assertEqual(all_values['foo2'], 'bar2')

import connection
import socket
import uuid
from twisted.internet import task
from scrapy.statscol import StatsCollector


# Default values.
STATS_UPDATE_INTERVAL = 0
STATS_KEY_PATTERN = '{spider}:stats:{id}'
STATS_KEY_EXPIRE = 0
STATS_SET_HOSTNAME = False


class RedisStatsCollector(StatsCollector):
    def __init__(self, crawler):
        super(RedisStatsCollector, self).__init__(crawler)
        self.server = connection.from_settings(crawler.settings)
        self.update_interval = crawler.settings.get('STATS_UPDATE_INTERVAL', STATS_UPDATE_INTERVAL)
        self.key_expire = crawler.settings.get('STATS_KEY_EXPIRE', STATS_KEY_EXPIRE)
        self.key_pattern = crawler.settings.get('STATS_KEY_PATTERN', STATS_KEY_PATTERN)
        self.hostname = socket.gethostname()
        self.key_name = None
        self.update_task = None
        
        if crawler.settings.get('STATS_SET_HOSTNAME', STATS_SET_HOSTNAME):
            self.set_value('hostname', self.hostname)

    def open_spider(self, spider):
        super(RedisStatsCollector, self).open_spider(spider)

        self.key_name = self.key_pattern.format(
            spider=spider.name,
            hostname=self.hostname,
            id=uuid.uuid4()
        )

        if not self.update_task and self.update_interval:
            self.update_task = task.LoopingCall(self._update_stats, spider)
            self.update_task.start(self.update_interval, now=True)

    def close_spider(self, spider, reason):
        super(RedisStatsCollector, self).close_spider(spider, reason)

        if self.update_task:
            self.update_task.stop()

    def _persist_stats(self, stats, spider):
        super(RedisStatsCollector, self)._persist_stats(stats, spider)
        self._update_stats(spider)

    def _update_stats(self, spider):
        self.server.hmset(self.key_name, self._stats)
        
        if self.key_expire:
            self.server.expire(self.key_name, self.key_expire)

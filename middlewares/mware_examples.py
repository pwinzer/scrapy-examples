# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

import logging
import hashlib
from base64 import b64encode
from w3lib.url import canonicalize_url
from urllib.parse import urlparse

from twisted.enterprise import adbapi
from scrapy import signals
from scrapy.utils.python import to_bytes
from scrapy.exceptions import IgnoreRequest

import my_project.es_logger as es_logger
from my_project.utils import get_db_args


class MyProjectProxyMiddleware(object):
    '''
    This DL MWare puts a proxy on outgoing requests
    '''
    # for this project to use ihire proxy
    def process_request(self, request, spider):
        proxy_addr = 'proxy-name.server.name:12345'
        
        #  choose proxy scheme based on request.url scheme
        parsed_url = urlparse(request.url)
        
        if parsed_url.scheme in ('http','https'):
            request.meta['proxy'] = f'{parsed_url.scheme}://{proxy_addr}'
        elif parsed_url.scheme == 'ftp':
            request.meta['proxy'] = f'https://{proxy_addr}'
        else:
            raise IgnoreRequest(f'request.url "{request.url}" does not contain valid scheme')
            

class ErrbackMiddleware(object):
    '''
    Ths DL Mware allows you to default an errback spider method defined in project/spiders/__init__.py
    
    The point is so that the deveopers don't have to specify an errback on each request within a spider
    If they do, the logs let them know they don't have to, for retry and redirect
        where requests are constructed from other requests and the errback is includeded
    '''
    def process_request(self, request, spider):
        if request.errback:
            if not request.meta.get('retry_times') and not request.meta.get('redirect_times'):
                spider.logger.warn(f'request {request} has assigned errback {request.errback} before ErrbackMiddleware')
        else:
            request.errback = spider.on_error


class EmptySpiderOutputMiddleware(object):
    '''
    This DL MWare posts an elasticsearch log if an execution of spider's callback doesn't yield a request or item
    
    This could be indicative of a Selector expression no longer working
    '''
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('EmptySpiderOutputMiddleware.__init__()')

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        is_empty_result = True
        
        for i in result:
            is_empty_result = False
            yield i
        
        if is_empty_result:
            spider.crawler.stats.inc_value('emptyspideroutputcount')
            log_name = 'empty-spider-output'
            log_msg = f'"{spider.name}" output was empty.'
            misc = {
                        'requesturl':           response.request.url,
                        'callbackdestination':  response.request.callback.__name__,
                    }
            
            r = es_logger.post_event(spider, log_name, log_msg, misc, level='Info')
            self.logger.info(f'es_logger {log_name} post returned {r.status_code}: {r.text}')


class SkipMiddleware(object):
    '''
    This DL MWare leverages unique indicies on SQL tables for persistent records of fetched urls
        so that they are not fetched more than once
    
    This is useful when visiting a page once forever is sufficients
    i.e. this is not useful if you plan to visit the same urls to get updated information
    '''
    # for skips
    def __init__(self, dbpool, skip_callback, skip_mode):
        supported_modes = ('OFF', 'SKIP', 'MEMO')
        
        self.hash_set = set()
        self.dbpool = dbpool
        self.logger = logging.getLogger(__name__)
        self.skip_callback_pattern = skip_callback
        
        mode = skip_mode.upper()
        if mode not in supported_modes:
            self.logger.warn(f"'{mode}' is not a supported SKIP_MWARE_MODE setting. "\
                             f"Defaulting to 'OFF'. Choose from one of {supported_modes}")
            mode = 'OFF'
            
        self.skip_mode = mode
        
    @classmethod
    def from_crawler(cls, crawler):
        dbargs = get_db_args()
        
        # ensure 8 connections for pool default is min=3 max=5 
        dbpool = adbapi.ConnectionPool('pyodbc', cp_min=8, cp_max=8,**dbargs)
        
        s = cls(
                dbpool,
                skip_callback = crawler.settings.get('SKIP_MWARE_CALLBACK_PATTERN'),
                skip_mode = crawler.settings.get('SKIP_MWARE_MODE'),
                )
        dbargs['APP'] = s.__class__.__name__
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s
    
    def process_request(self, request, spider):
        # returning None (or no return statement) means request just passed through to next mware
        if (self.skip_callback_pattern in request.callback.__name__) and (self.skip_mode != 'OFF'):
            hash_val, canon_url = self._get_hash(request.url)
            
            if hash_val in self.hash_set:
                self.logger.warn(f'inmemoryurlduplicate {request.url} is already queue for request')
                spider.crawler.stats.inc_value('inmemoryurlduplicatecount')
                raise IgnoreRequest()
            
            self.hash_set.add(hash_val)
            d = self.dbpool.runInteraction(self._do_normalized_upsert, request, spider, canon_url, hash_val)
            return d
    
    def _do_normalized_upsert(self, conn, request, spider, canon_url, hash_val):
        '''
        SourcePages relates Sources (spiders in db) to Pages (usually identified with URL but sometimes compositely)
        '''
        
        q = """
            SELECT
                SP.SourcePageId,
                P.PageId,
                SP.SkipCount
            FROM Pages P
            LEFT JOIN SourcePages P ON P.PageId = SP.PageId AND SP.SourceId = ?
            WHERE I.Base64SHA1Hash = ?
            """
        params = (spider.source_id, hash_val)
        conn.execute(q, params)
        row = conn.fetchone()
        
        source_page_id = None
        page_id = None
        
        if row:
            source_page_id = row[0]
            page_id = row[1]
            skip_count = row[2]
        
            self.logger.debug(f'source_page_id : {source_page_id}')
            self.logger.debug(f'page_id : {page_id}')
            self.logger.debug(f'skip_count : {skip_count}')
            
        if source_page_id:
            # dupe combo exists for this Source and URL hash
            
            # update db and ignore it
            if self.skip_mode == 'SKIP':
                self.logger.debug('SKIPPING')
                q = """
                    UPDATE SourcePages
                    SET LastSkippedDate = GETDATE(), SkipCount = ?
                    WHERE SourcePageId = ?
                    """
                params = (skip_count + 1, source_page_id)
                conn.execute(q, params)
                self.hash_set.remove(hash_val)
                raise IgnoreRequest()
            
            # take the source_page_id along for memo-ing 
            elif self.skip_mode == 'MEMO':
                self.logger.debug('Requesting page to MEMO')
                request.meta['source_page_id'] = source_page_id
        
        elif not page_id:
            # URL hash doesn't exists at all
            # insert it and take the page_id along for the ride to process_response
            
            # There is also a Name column on Pages, with Default value 'URL'
            # To flex this, we can leverage request.meta['page_identifier_names'] = ('URL', 'OtherKey')
            #   or request.meta['page_identifiers'] = {'URL':'www.site.com', 'OtherKey':'spam'}
            q = """
                INSERT INTO Pages (Base64SHA1Hash, Value)
                OUTPUT inserted.PageId
                VALUES (?,?)
                """
            params = (hash_val, canon_url)
            conn.execute(q, params)
            page_id = conn.fetchone()[0]
            self.logger.debug(f'Pages.PageId just inserted: {page_id}')
            
        request.meta['hash_val'] = hash_val
        request.meta['page_id'] = page_id
        
    def _get_hash(self, url):
        canon_url = to_bytes(canonicalize_url(url))
        h = hashlib.sha1()
        h.update(canon_url)
        hash_val = b64encode(h.digest())
        return hash_val, canon_url
    
    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        if self.skip_mode == 'OFF' or self.skip_callback_pattern not in request.callback.__name__:
            return response
        
        source_page_id = request.meta.get('source_page_id')
        page_id = request.meta['page_id']
        hash_val = request.meta['hash_val']
        
        if response.status != 200:
            # don't let hash_set bloat with 301s, 404s, 502s, ConnectionLost, etc
            self.logger.info('Skip Mware removing hash of non-200 response')
            self.hash_set.remove(hash_val)
            return response
        
        if source_page_id:
            if self.skip_mode == 'SKIP':
                self.logger.error('SourcePageId present while processing a Response in SKIP mode. This code should be unreachable')
                self.hash_set.remove(hash_val)
                raise IgnoreRequest()

            # MEMO
            q = """
                UPDATE SourecePages
                SET LastVisitDate = GETDATE()
                WHERE SourcePageId = ?
                """
            params = (source_page_id)
            d = self.dbpool.runOperation(q, params)
        
        else:
            self.logger.debug(f'request.meta : {request.meta}')
            
            q = """
                INSERT INTO SourcePages
                (PageId, SourceId, FirstVisitDate, LastVisitDate)
                VALUES (?, ?, GETDATE(), GETDATE())
                """
            params = (page_id, spider.source_id)
            d = self.dbpool.runOperation(q, params)
        
        d.addCallback(self._remove_hash, hash_val)
        d.addCallback(lambda _: response) # return response on success
        return d
            
    def _remove_hash(self, result, hash_val):
        self.logger.debug(f'removing {hash_val} from self.hash_set')
        self.hash_set.remove(hash_val)
        
    def spider_closed(self, spider):
        self.dbpool.close()
        

#!/usr/bin/python3
import praw
import feedparser
import configparser
import time
import hashlib
import logging
import logging.handlers
import os
import random
import html2text
import queue
import signal
import sys
import threading
import re
import xxhash
from threading import Thread, RLock
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup as BS



def split(s):
    """Split a string in sensible places to fit reddit's comment limit"""
    s2 = "ERROR ERROR"
    if len(s) > 10000:
        s1, s2 = s[:len(s) // 2], s[len(s) // 2:]
        s1nl = s1.rfind('\n')
        s2nl = s2.find('\n')
        if s2nl < len(s1) - s1nl:
            s1 += s2[:s2nl]
            s2 = s2[s2nl:]
        else:
            s2 = s1[s1nl:] + s2
            s1 = s1[:s1nl]
        s2 = 'Continued:\n\n>' + s2
        return s1, s2
    else:
        return s


class FeedReader(Thread):
    """A Thread implementation of an RSS feed reader.
    Constantly checks a known RSS feed for new posts. Upon finding such a post, it will encapsulate it
    in a Post object and place it in the queue for the Poster threads to post.
    Additionally handles scraping HTML off of news blogs, but does -not- handle the markdown conversion.
    """
    instances = 0

    def __init__(self, feed_link, queue, hasher, logger, is_blog):
        """Creates the new FeedReader object with given parameters"""
        FeedReader.instances += 1
        super().__init__(name='Reader-' + str(FeedReader.instances))
        # Make sure we don't overwhelm the poor server
        os.nice(10)
        # Set everything up
        self.feed_link = feed_link
        self.feed = feedparser.parse(self.feed_link)
        self.q = queue
        self.hasher = hasher
        self.logger = logger
        self.is_blog = is_blog
        self.running = True

    def run(self):
        """Constantly check for new posts in the feed"""
        self.logger.debug(threading.current_thread().name + ' has begun reading')
        while self.running:
            # Parse the feed into a readable object
            self.feed = feedparser.parse(self.feed_link)
            if len(self.feed.entries) > 0:
                # Make sure we're working with up-to-date hashes (this is thread safe)
                self.hasher.load_hashes()
                for i in range(200):
                    try:
                        # TODO: Investigate why the hashing isn't 100%
                        post_hash = xxhash.xxh64(
                            self.feed.entries[i].description.encode()
                        ).hexdigest()

                        # If this post is new to us...
                        if post_hash not in self.hasher.hashes:
                            self.logger.info('New post found by ' + threading.current_thread().name +
                                             ': ' + self.feed.entries[i].title)
                            desc = None
                            # If we're one of the readers that read from the blog, we need to do
                            #  some extra work before passing off the post
                            if self.is_blog:
                                page = urlopen(self.feed.entries[i].link)
                                soup = BS(page)
                                # Grab -just- the part of the page we want
                                blog = soup.find('div', class_='news-detail')
                                desc = ''.join(str(x) for x in blog)
                            else:
                                desc = self.feed.entries[i].description
                            # Put the Post in the queue!
                            self.q.put(Post(
                                self.feed.entries[i].title,
                                self.feed.entries[i].link,
                                desc,
                            ))
                            self.hasher.store_hash(post_hash)
                            self.logger.debug(threading.current_thread().name +
                                              ' successfully submitted: ' + self.feed.entries[i].title +
                                              ' to the queue')
                    # Sweep the possibility of going outside of the bounds of the feed under the rug
                    except IndexError:
                        break
            else:
                time.sleep(5)
        self.logger.debug(threading.current_thread().name + ' is stopping')

    def stop(self):
        self.running = False


class Hasher:
    """Singleton (not really) class for storing and retrieving post hashes"""
    def __init__(self, num_hashes, folder):
        """Set up a new Hasher"""
        self.num = num_hashes
        self.folder = folder
        self.hashes = set()
        self.lock = RLock()

    def hash_filename(self, i):
        """Helper function to turn a hash index into a valid filename"""
        return os.path.join(self.folder, 'hash' + str(i) + '.txt')

    def load_hashes(self):
        """Reload all hashes from files into the set"""
        with self.lock:
            self.hashes = set()
            for i in range(self.num):
                if os.path.isfile(self.hash_filename(i)):
                    with open(self.hash_filename(i)) as f:
                        self.hashes.add(f.read())

    def is_hashed(self, hash):
        """Determine if a hash is already present in the set"""
        return hash in self.hashes

    def store_hash(self, hash):
        """Given the hash of a post, store it where it should go"""
        with self.lock:
            for i in range(self.num - 1, -1, -1):
                if os.path.isfile(self.hash_filename(i)):
                    os.rename(self.hash_filename(i), self.hash_filename(i + 1))
            if os.path.isfile(self.hash_filename(self.num)):
                os.remove(self.hash_filename(self.num))
            with open(self.hash_filename(0), 'w') as h:
                h.write(hash)
            self.load_hashes()


class Post:
    """Container class for posts, consisting of a title, link, and full text"""
    def __init__(self, title, link, text):
        self.title = title
        self.link = link
        self.text = text


class Poster(Thread):
    """Thread class for posting blogs to reddit"""
    instances = 0

    def __init__(self, q, logger, config):
        """Set up a new Poster"""
        Poster.instances += 1
        super().__init__(name='Poster-' + str(Poster.instances))
        # Keep the server from bursting into flame
        os.nice(10)

        # Instance a html->markdown converter
        self.h = html2text.HTML2Text(bodywidth=0)
        self.h.protect_links = True
        self.running = True
        self.q = q
        self.logger = logger

        # Load up important things, connect to reddit
        self.sub = config.get('STOBot', 'subreddit')
        self.reddit = praw.Reddit(config.get('STOBot', 'agent'))
        self.reddit.set_oauth_app_info(
            client_id=config.get('OAuth', 'client_id'),
            client_secret=config.get('OAuth', 'client_secret'),
            redirect_uri=config.get('OAuth', 'redirect_uri')
        )
        with open(config.get('STOBot', 'tokenFile')) as f:
            self.token = f.read()
        self.refresh_time = None
        self.auth = False
        self.reauth()

    def reauth(self):
        """Reauthorize our reddit connection"""
        self.auth = False
        fail = False
        # Failure is unacceptable.
        while not self.auth:
            try:
                self.reddit.refresh_access_information(self.token)
                self.refresh_time = time.time()
                self.auth = True
                if fail:
                    self.logger.warning('Reauth success!')
            except (praw.errors.PRAWException, praw.errors.HTTPException) as e:
                fail = True
                self.logger.warning('Reauth failed, retrying in 5')
                time.sleep(5)
                continue

    def run(self):
        """Main thread running loop"""
        self.logger.debug(threading.current_thread().name + ' has begun looking for posts to make')
        while self.running:
            # Preemptively refresh the auth
            if time.time() - self.refresh_time > 3400:
                self.reauth()
            try:
                # Wait around a while for a new post
                p = self.q.get(timeout=3400)
                if p is None:
                    break
                self.post(p.title, p.link, p.text)
                self.q.task_done()
            except queue.Empty:
                pass
        self.logger.debug(threading.current_thread().name + ' is stopping')

    def post(self, title, link, text):
        """Actually post the post"""
        self.logger.debug(threading.current_thread().name + ' is submitting ' + title)
        text = self.h.handle(text)
        # Put meaningful text in what html2text turns inline images into.
        text = text.replace("[![]", "[(Linked Image) ![]")

        # Prepend desc, further sanitize images, and enclose the rest of the text in reddit quote blocks
        text = 'Post contents:\n\n>' + text.replace('![]', '[Image]')\
            .replace('\n', '\n>')
        text = [text]
        # Make -dern- sure that we're below the reddit comment limit
        while any(len(s) > 10000 for s in text):
            text = [x for s in text for x in split(s)]
        try:
            # POST!
            submission = self.reddit.submit(self.sub, title, url=link, resubmit=False)

            # This should never happen, but if we didn't get a valid Submission object, we can find it
            # manually
            if not isinstance(submission, praw.objects.Submission):
                id = urlparse(submission).path.split('/')[4]
                submission = self.reddit.get_submission(submission_id=id)
                self.logger.warning('reddit.submit failed to return a Submission')

            # Comment on comments
            comment = submission.add_comment(text.pop(0))
            for s in text:
                comment = comment.reply(s)
            self.logger.info(threading.current_thread().name + ' successfully submitted ' + title + ' to ' + self.sub)
        except praw.errors.AlreadySubmitted:
            self.logger.warning('Attempted to repost!')
        except praw.errors.HTTPException:
            self.logger.warning('HTTP error occurred!')

    def stop(self):
        """Cease"""
        self.running = False


class Bot:
    """Main bot class

    Handles config reading and thread instantiation.
    """
    def __init__(self, inifile):
        """Program startup"""
        self.config = configparser.ConfigParser()
        self.config.read(inifile)
        self.logger = logging.getLogger('BotLogger')
        self.logger.setLevel(logging.INFO)

        handler = logging.handlers.RotatingFileHandler(
            self.config.get('STOBot', 'logFile'),
            maxBytes=self.config.getint('STOBot', 'logSize'),
            backupCount=self.config.getint('STOBot', 'logFiles')
        )
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(handler)

        # Load importan things
        self.hasher = Hasher(self.config.getint('STOBot', 'hashFiles'),
                             self.config.get('STOBot', 'hashFolder'))

        self.blog_feed_link = self.config.get('STOBot', 'blogFeed')
        self.tribble_feed_link = self.config.get('STOBot', 'tribbleFeed')

        self.xb_feed_link = self.config.get('STOBot', 'xboneFeed')
        self.ps_feed_link = self.config.get('STOBot', 'psFeed')

        self.feeds = [
            self.blog_feed_link,
            self.xb_feed_link,
            self.ps_feed_link
        ]

        # Instantiate one (1) Queue
        self.queue = queue.Queue()
        self.quotes = None

        self.posters = []

        # Generate Poster threads
        for i in range(self.config.getint('STOBot', 'numThreads')):
            t = Poster(self.queue, self.logger, self.config)
            t.start()
            self.posters.append(t)

        self.readers = []

        # Generate Reader threads
        for f in self.feeds:
            self.readers.append(FeedReader(
                f,
                self.queue,
                self.hasher,
                self.logger,
                True
            ))

        self.readers.append(FeedReader(
            self.tribble_feed_link,
            self.queue,
            self.hasher,
            self.logger,
            False
        ))
        for t in self.readers:
            t.start()

        self.logger.info('Initialization complete')

    def handle_exit(self, signal, frame):
        """If for some reason we receive a SIGINT, try to die gracefully"""
        for t in self.readers:
            t.stop()
        for t in self.posters:
            t.stop()
        self.queue.join()
        sys.exit(0)

# Application entry point
if __name__ == "__main__":
    random.seed()
    b = Bot('stobot.ini')
    signal.signal(signal.SIGINT, b.handle_exit)

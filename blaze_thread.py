#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# blaze_thread (c) 2016 by Andre Karlsson<andre.karlsson@protractus.se>
#
# This file is part of blaze_thread.
#
#    blaze_thread is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Foobar is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# Filename:  by: andrek
# Timesamp: 4/28/16 :: 11:14 PM

# -*- coding: utf-8 -*-

import sys
import time
import threading
import bisect
import itertools
import inspect


class BaseThread(object):
	def __init__(self, priority=1):
		self.id = id(self)
		self.ticks = 0
		self.cpu = 0
		self.score = 0
		self.priority = priority
		self.sleep_started_at = 0
		self.time_to_sleep = 0
		self.wakeup_at = 0
		self.last_executed_at = 0
		self.created_at = time.time()
		self.dead = False

	def start(self):
		self.dead = False
		self.is_generator = inspect.isgeneratorfunction(self.process)
		if self.is_generator:
			self.generator = self.process()

	def process():
		raise NotImplementedError

	def stop(self):
		self.dead = True

	def prepare_for_sleep(self, time_to_sleep):
		self.sleep_started_at = time.time()
		self.time_to_sleep = time_to_sleep
		self.wakeup_at = self.sleep_started_at + time_to_sleep

	@property
	def waiting_time_delta(self):
		return time.time() - max(self.created_at, self.wakeup_at, self.last_executed_at)

	def execute(self):
		if self.is_generator:
			return self.generator.next()
		else:
			return self.process()

	def run(self):
		retvalue = None
		try:
			started_at = time.time()
			retvalue = self.execute()
			finished_at = time.time()
			elapsed = finished_at - started_at
			self.ticks += 1
			self.cpu += elapsed
			self.score += elapsed * 1 / self.priority
			self.last_executed_at = finished_at
			if self.dead:
				raise StopIteration
		except StopIteration:
			retvalue = None

		if retvalue >= 0:
			self.prepare_for_sleep(retvalue)

		return retvalue

	def __repr__(self):
		return 'Thread(%d): ticks=%d time=%0.4f score=%0.4f' % (self.id, self.ticks, self.cpu, self.score)


class BaseQueue(object):
	def __init__(self, sorting_key):
		self._threads = []
		self._key = sorting_key

	@property
	def empty(self):
		return len(self.threads) == 0

	@property
	def threads(self):
		return self._threads

	def append(self, thread):
		self.reappend(thread)

	def remove(self, thread):
		self.threads.remove(thread)

	def reappend(self, thread):
		score = getattr(thread, self._key)
		position = bisect.bisect_right(self.scores, score)
		self.threads.insert(position, thread)


class SleepingQueue(BaseQueue):
	def __init__(self):
		super(SleepingQueue, self).__init__(sorting_key='wakeup_at')

	@property
	def scores(self):
		return [t.wakeup_at for t in self.threads]

	@property
	def next_wakeup(self):
		try:
			return self._threads[0].wakeup_at
		except IndexError:
			return 0

	@property
	def wakeup_threads(self):
		wakedup = []
		while 1:
			try:
				thread = self.threads[0]
				if thread.wakeup_at <= time.time():
					wakedup.append(self.threads.pop(0))
				else:
					break
			except IndexError:
				break
		return iter(wakedup)


class RunningQueue(BaseQueue):
	def __init__(self):
		super(RunningQueue, self).__init__(sorting_key='score')

	def append(self, thread):
		if self.empty:
			thread.score = 0
		else:
			thread.score = self.threads[0].score
		super(RunningQueue, self).append(thread)

	@property
	def scores(self):
		return [t.score for t in self.threads]

	def pop_next_thread(self):
		try:
			return self._threads.pop(0)
		except IndexError:
			return None


class Scheduler(object):
	def __init__(self, idle_sleep_interval=0.01):
		self._running_queue = RunningQueue()
		self._sleeping_queue = SleepingQueue()
		self._stopped = False
		self._lock = threading.Lock()
		self.waiting_time_delta = 0
		self.switches_count = 0
		self.max_threads = 0
		self.idle_sleep_interval = idle_sleep_interval

	@property
	def run_queue(self):
		return self._running_queue

	@property
	def sleep_queue(self):
		return self._sleeping_queue

	@property
	def threads(self):
		return self.run_queue.threads + self.sleep_queue.threads

	@property
	def empty(self):
		return self.run_queue.empty and self.sleep_queue.empty

	@property
	def running(self):
		return not self._stopped

	def add(self, thread):
		with self._lock:
			if self.running:
				thread.start()
				self.run_queue.append(thread)
				self.max_threads = max(self.max_threads, len(self.threads))

	def sleep(self):
		time.sleep(self.idle_sleep_interval)

	def wait_for_sleep_thread(self):
		while self.running and self.run_queue.empty and self.sleep_queue.next_wakeup > time.time():
			self.sleep()

	def wait_for_new_thread(self):
		while self.running and self.run_queue.empty:
			self.sleep()

	def wait_for_thread(self):
		if not self.sleep_queue.empty:
			self.wait_for_sleep_thread()
		else:
			self.wait_for_new_thread()

	def wakeup_threads(self):
		if self.sleep_queue.next_wakeup <= time.time():
			for thread in self.sleep_queue.wakeup_threads:
				self.run_queue.append(thread)

	def get_next_thread(self):
		self.wait_for_thread()
		self.wakeup_threads()
		return self.run_queue.pop_next_thread()

	def stop(self):
		self._stopped = True

	def stop_all_threads(self):
		for thread in self.threads:
			thread.stop()

	def process(self):
		thread = self.get_next_thread()
		if thread:
			delta = thread.waiting_time_delta
			self.waiting_time_delta = (self.waiting_time_delta * self.switches_count + delta) / (
			self.switches_count + 1)
			self.switches_count += 1
			value = thread.run()
			if not value is None:
				if value == 0:
					self.run_queue.reappend(thread)
				else:
					self.sleep_queue.append(thread)

	def run(self):
		while self.running:
			self.process()
		self.stop_all_threads()
		while not self.empty:
			self.process()

	def __repr__(self):
		return 'Scheduler: avg wait time: %f total switches: %0.4f max threads: %d' % (
		self.waiting_time_delta, self.switches_count, self.max_threads)


if __name__ == "__main__":
	import random

	scheduler = Scheduler()


	class RunOnce(BaseThread):
		def __init__(self, id):
			super(RunOnce, self).__init__()
			self.id = id
			self.last_logged = time.time()

		def log(self, str):
			print
			"id: %d, time=%s, elapsed=%f event - %s" % (self.id, time.time(), time.time() - self.last_logged, str)

		def process(self):
			self.log('started')
			for i in xrange(1000):
				pass
			self.log('sleep 0')
			yield 0
			self.log('wakeup')
			for i in xrange(1000):
				pass
			sleep = random.randint(1, 2)
			self.log('sleep %d' % sleep)
			yield sleep
			self.log('wakeup')
			for i in xrange(1000):
				pass
			self.log('finished')


	class RunLoop(BaseThread):
		def __init__(self, id):
			super(RunLoop, self).__init__()
			self.id = id
			self.last_logged = time.time()

		def log(self, str):
			print
			"id: %d, time=%s, elapsed=%f event - %s" % (self.id, time.time(), time.time() - self.last_logged, str)
			self.last_logged = time.time()

		def process(self):
			self.log('started')
			while not self.dead:
				sleep = random.randint(1, 3)
				self.log('sleep %d' % sleep)
				yield sleep
				self.log('wakeup')
			self.log('finished')


	for i in xrange(5):
		thread = RunOnce(i)
		scheduler.add(thread)

	for i in xrange(50, 55):
		thread = RunLoop(i)
		scheduler.add(thread)

	threading.Timer(5, lambda: scheduler.stop()).start()
	threads = scheduler.threads

	scheduler.run()

	print
	scheduler
	for t in threads:
		print
		t

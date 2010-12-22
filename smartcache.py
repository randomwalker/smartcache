#!/usr/bin/python
from __future__ import with_statement

import time, os.path, hashlib, os, decorator, memcache, MySQLdb, cPickle, logging, copy, random, threading, base64, traceback

from datetime import datetime

"""You need to create tables manually in your database with this schema:
create table smartcache (
	func varchar(64),
	version char(16),
	domain varchar(64),
	thekey char(16) not null primary key,
	effort float,
	modified timestamp,
	expires_hint datetime,
	value mediumblob
);

create table smartstore (
	prefix varchat(128),
	thekey char(16) not null primary key,
	modified timestamp,
	value mediumtext
);

"""

def sqlLock():
	"""currently a dummy lock"""
	return threading.Lock()

def setLogLevel(level=logging.ERROR):
	_logger.setLevel(level)
	_logger.handlers[0].setLevel(level)

def locked(lock=None):
	if not lock:
		lock = threading.Lock()
	@decorator.decorator
	def _locked(func, *args, **kwargs):
		with lock:
			return func(*args, **kwargs)
	return _locked

class Immutable:
	"""if you want the key of this object never to change after creation, 
		simply inherit from this class."""
	@property
	@locked()
	def __scid__(self):
		if not hasattr(self, "__scid__real"):
			#random id, generated on first access
			self.__scid__real = "%x" % random.randrange(1<<64) 
		return self.__scid__real

class Serializable:
	def __init__(self, kwdic):
		self.__dict__ = kwdic

class Config:
	def __init__(self, **kwargs):
		"""recompute: yes/no/auto/defer/lookup/alwaysdefer -- what to do if cache miss
		default -- what to return if cache miss and recompute is no"""
		self.__dict__ = kwargs
		self.default = None
		if kwargs.get('bypasscache'):
			self.use_mc = self.use_db = False
		
	def setDefaults(self, **kwargs):
		for k, v in kwargs.items():
			if not hasattr(self, k):
				setattr(self, k, v)

def summarizeArgs(args):
	"""helper function to print an argument list"""
	def summarizeArg(arg):
		if hasattr(arg, '__scid__'):
			try:
				return '%s<%s>' % (arg.__class__.__name__, arg.__scid__)
			except:
				return repr(arg)
		return repr(arg)
	return '(%s)' % ", ".join(map(summarizeArg, args))

def init(name, mcaddr=['127.0.0.1:11211'], cursorthunk=None, logger=logging.getLogger()):
	"""name: name of your project. make this different for different projects, 
	otherwise files will overwrite each other.
	
	logdir: where you want log files to go
	
	mcaddr: list of addresses where memcached is listening
	
	cursor_thunk: 	a callable that returns a cursor

			note that you need to create the database and smartcache table manually.
	"""
	global _projectname, _mcclient, _cursor_thunk, _logger
	if hasattr(init, 'done'):
		return
	init.done = True
	_projectname = name
	_mcclient = memcache.Client(mcaddr, debug=0)
	_cursor_thunk = cursorthunk
	_logger = logger

def getCursor():
	return _cursor_thunk()

def mcConfig(**kwargs):
	global _mcclient
	_mcclient = memcache.Client(**kwargs)

def funcToStr(func):
	"""if filename argument is supplied to smartCache() then it will be used here,
	else func.__module__ will be used"""
	if not func: return ""
	module = os.path.basename(func.__file__).split('.')[0] 	if func.__file__ \
															else func.__module__
	return module + '.' + func.__name__

def deriveKey(func, prefix, version, args):
	"""96 bit base 64 encoded key"""
	def objKey(obj):
		#deliberately treat object with __scid__ id as identical to string with value id
		if hasattr(obj, "__scid__"):
			return str(obj.__scid__)
		else:
			try:
				return str(obj)
			except:
				#FIXME: warn on errors
				return unicode(obj).encode('utf-8', 'replace')
	return base64.urlsafe_b64encode(hashlib.sha1(repr(
				["smartcache", prefix, str(version), funcToStr(func)] +
				map(objKey, args)))
			.digest())[:16]

class SmartStore:
	"""this is a simple key/value datastore with multiple backends. nothing fancy."""
	def __init__(self, prefix, version="", default=None, use_mc=True, use_db=True):
		"""prefix: the type of key. example: "userid" """
		for k, v in locals().items():
			if k != "self":
				setattr(self, k, v)

	def __setitem__(self, key, value):
		thekey = deriveKey( func=None,
							prefix=_projectname+self.prefix,
							version=self.version,
							args=[key])
		if self.use_mc:
			_mcclient.set(thekey, value)
		if self.use_db:
			sqlvalue = cPickle.dumps(value, cPickle.HIGHEST_PROTOCOL)
			cursor = getCursor()
			with sqlLock():
				cursor.execute("""insert into smartstore(prefix, thekey, value) values
								(%s, %s, %s)
								on duplicate key update value=%s""",
								(self.prefix, thekey, sqlvalue, sqlvalue))

	def __getitem__(self, key):
		thekey = deriveKey( func=None, 
							prefix=_projectname+self.prefix,
							version=self.version,
							args=[key])
		result = _mcclient.get(thekey) if self.use_mc else None
		if result is None and self.use_db:
			cursor = getCursor()
			with sqlLock():
				cursor.execute("select value from smartstore where thekey=%s", thekey)
				try:
					result = cPickle.loads(cursor.fetchone()[0])
				except:
					result = None
		if result is None: return self.default
		return result


def parseOpts(args):
	"""returns opts, realargs where the last arg of args might be opts"""
	if len(args) == 0 or not isinstance(args[-1], Config):
		return Config(), args
	return args[-1], args[:-1]

def acquireRowLock(key):
	cursor = getCursor()
	cursor.execute('select deferred from smartcache where thekey=%s', key)
	try:
		deferred = cursor.fetchone()[0]
	except TypeError:
		#FIXME: this occurs when the key is not found i.e., alwaysdefer
		#there is currently no mechanism to lock when using alwaysdefer
		return True
	if deferred:     #already locked
		return False
	else:
		cursor.execute( 'update smartcache set deferred=%s where thekey=%s', 
						(datetime.now(), key))
		return True

def releaseRowLock(key):
	cursor = getCursor()
	cursor.execute('update smartcache set deferred=%s where thekey=%s', (None, key))

class Worker(threading.Thread):
	def __init__(self, execute, func, args, opts):
		threading.Thread.__init__(self)
		self.execute = execute
		self.func = func
		self.args = args
		self.opts = opts
	
	def run(self):
		opts = copy.copy(self.opts)
		opts.recompute = "yes"
		try:
			self.execute(self.func, self.args, opts)
		except:
			error_prefix = 'exception in deferred thread\t%s%s\t' % \
							(self.func.__name__, summarizeArgs(self.args))
			for tbline in traceback.format_exc().split('\n'):
				_logger.error(error_prefix + tbline)
		if self.opts.use_db:
			key = deriveKey(self.func, _projectname, self.opts.version, self.args)
			releaseRowLock(key)
			

def smartCache(version=0, effort=1.0, expiry=None, filename=None, use_mc=True, use_db=True, recompute="auto", resultclass=None):
	"""this is the main decorator.
	
		expiry is the number of seconds to cache a result. None --> never expires
		example: "10m" --> 10 minutes, "1h" --> 1 hour, "30d" --> 30 days 
		
		use_mc: use memcached
		use_db: use mysql cache
		version: arbitrary value. change this when your code changes and you want to 
				expire the cache for all values of this function."""
	def expTime(expstring):
		"""converts expiry time from human readable format to seconds"""
		if not expstring: return None
		if isinstance(expstring, str) or isinstance(expstring, unicode):
			for unit, mult in {"m" : 60, "h": 3600, "d": 86400}.items():
				expstring = expstring.replace(unit, "*"  + str(mult))
			return eval(expstring)
		else:
			return expstring

	def pastExpiry(expstring, modtime):
		return expstring is not None and expTime(expstring) + modtime < time.time()

	def getCached(key, opts):
		"""returns (value, modtime, source); 
		(None, None, None) if not found"""
		if opts.use_mc:
			result = _mcclient.get(key)
			if result:
				modtime, value = result
				if resultclass:
					value = resultclass(value)
				return value, modtime, "mc"
		if opts.use_db:
			cursor = getCursor()
			with sqlLock():
				cursor.execute("select modified, value from smartcache where thekey=%s",
								key)
				result = cursor.fetchone()
			if result:
				sqlmodtime, sqlvalue = result
				modtime = time.mktime(sqlmodtime.timetuple())
				value = cPickle.loads(sqlvalue)
				if resultclass:
					value = resultclass(value)
				return value, modtime, "db"
		return None, None, None

	def setCache(key, value, modtime, source, opts, funcstr):
		if resultclass:
			value = value.__dict__
		if opts.use_db and source == "func":
			sqlvalue = cPickle.dumps(value, protocol=cPickle.HIGHEST_PROTOCOL)
			#FIXME: make expires_hint null if not expiring
			sqlexptime = datetime.fromtimestamp(time.time() + 
							(expTime(opts.expiry)  if opts.expiry else 3.0e9))
			cursor = getCursor()
			with sqlLock():
				cursor.execute(""" insert into smartcache 
								(func, version, domain, thekey, effort, modified, expires_hint, value)
								values (%s, %s, %s, %s, %s, %s, %s, %s) 
								on duplicate key update 
								expires_hint=%s, value=%s""",
								(funcstr, version, _projectname, key, 
								opts.effort, datetime.now(), sqlexptime, sqlvalue, 
								sqlexptime, sqlvalue))
		if opts.use_mc and source in ["func", "db"]:
			_mcclient.set(key, (time.time(), value))

	def writeLog(func, args, key, modtime, source, opts):
		#if source == "func":
		status = { 	'func': 'RCMPT',
					'mc'  : 'MC',
					'db'  : 'DB',
					None  : 'MISS'
		}[source]
		levelname = { 	'func': 'info',
						'mc'  : 'debug',
						'db'  : 'info',
						None  : 'debug'
		}[source]
		levelfunc = getattr(_logger, levelname)
		levelfunc("thread:%s\t%s\t%s\tversion=%s	key=%s	%s%s" % 
			(threading.currentThread().getName(), opts.recompute, status,
			opts.version, key, func.__name__, summarizeArgs(args)))
		
	def reCompute(func, args, opts):
		if opts.recompute == "no":
			#FIXME: raise exception
			return None, None, None
		return func(*args), time.time(), "func"

	@decorator.decorator
	def _cache(func, *args):
		opts, args = parseOpts(args)
		opts = copy.copy(opts)
		func.__file__ = filename
		opts.setDefaults(version=version, effort=effort, filename=filename, expiry=expiry, 
					use_mc=use_mc, use_db=use_db, recompute=recompute)
		return execute(func, args, opts)


	def execute(func, args, opts):
		key = deriveKey(func, _projectname, opts.version, args)
		if opts.recompute == "yes":
			value, modtime, source = reCompute(func, args, opts)
		else:
			value, modtime, source = getCached(key, opts)
		
		if not source or pastExpiry(opts.expiry, modtime):
			if opts.recompute == "alwaysdefer" or (source and opts.recompute == "defer"):
				lock = acquireRowLock(key) if opts.use_db else True
				if lock:
					Worker(execute, func, args, opts).start()
			elif opts.recompute == "lookup":
				pass
			else: #recompute
				value, modtime, source = reCompute(func, args, opts)
		
		if source:
			setCache(key, value, modtime, source, opts, funcToStr(func))
		writeLog(func, args, key, modtime, source, opts)

		if source:
			return value
		else:
			return opts.default

	return _cache


/*!
 * kue
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
  , Worker = require('./queue/worker')
  , events = require('./queue/events')
  , Job = require('./queue/job')
  , redis = require('./redis');

/**
 * Expose `Queue`.
 */

exports = module.exports = Queue;

/**
 * Library version.
 */

exports.version = '0.6.2';

/**
 * Expose `Job`.
 */

exports.Job = Job;

/**
 * Server instance (that is lazily required)
 */

var app;

/**
 * Expose the server.
 */

Object.defineProperty(exports, 'app', {
  get: function() {
    return app || (app = require('./http'));
  }
});

/**
 * Expose the RedisClient factory.
 */

exports.redis = redis;

/**
 * Create a new `Queue`.
 *
 * @return {Queue}
 * @api public
 */

exports.createQueue = function(){
  return Queue.singleton = new Queue;
};

/**
 * Initialize a new job `Queue`.
 *
 * @api public
 */

function Queue() {
  this.client = redis.createClient();
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Queue.prototype.__proto__ = EventEmitter.prototype;

/**
 * Create a `Job` with the given `type` and `data`.
 *
 * @param {String} type
 * @param {Object} data
 * @return {Job}
 * @api public
 */

Queue.prototype.create =
Queue.prototype.createJob = function(type, data){
  return new Job(type, data);
};

/**
 * Proxy to auto-subscribe to events.
 *
 * @api public
 */

var on = EventEmitter.prototype.on;
Queue.prototype.on = function(event){
  if (0 == event.indexOf('job')) events.subscribe();
  return on.apply(this, arguments);
};

/**
 * Promote delayed jobs, checking every `ms`,
 * defaulting to 5 seconds.
 *
 * @params {Number} ms
 * @api public
 */

Queue.prototype.promote = function(ms){
  var client = this.client
    , ms = ms || 5000
    , limit = 20;

  setInterval(function(){
    client.sort(redis.keyPrefix + 'jobs:delayed'
      , 'by', redis.keyPrefix + 'job:*->delay'
      , 'get', '#'
      , 'get', redis.keyPrefix + 'job:*->delay'
      , 'get', redis.keyPrefix + 'job:*->created_at'
      , 'limit', 0, limit, function(err, jobs){
      if (err || !jobs.length) return;

      // iterate jobs with [id, delay, created_at]
      while (jobs.length) {
        var job = jobs.slice(0, 3)
          , id = job[0]
          , delay = parseInt(job[1], 10)
          , creation = parseInt(job[2], 10)
          , promote = ! Math.max(creation + delay - Date.now(), 0);

        // if it's due for activity
        // "promote" the job by marking
        // it as inactive.
        if (promote) {
          Job.get(id, function(err, job){
            if (err) return;
            events.emit(id, 'promotion');
            job.inactive();
          });
        }

        jobs = jobs.slice(3);
      }
    });
  }, ms);
};

/**
 * Get setting `name` and invoke `fn(err, res)`.
 *
 * @param {String} name
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.setting = function(name, fn){
  this.client.hget(redis.keyPrefix + 'settings', name, fn);
  return this;
};

/**
 * Process jobs with the given `type`, invoking `fn(job)`.
 *
 * @param {String} type
 * @param {Number|Function} n
 * @param {Function} fn
 * @api public
 */

Queue.prototype.process = function(type, n, fn){
  var self = this;

  if ('function' == typeof n) fn = n, n = 1;

  while (n--) {
    (function(worker){
      worker.on('error', function(err){
        self.emit('error', err);
      });

      worker.on('job complete', function(job){
        self.client.incrby(redis.keyPrefix + 'stats:work-time', job.duration);
      });
    })(new Worker(this, type).start(fn));
  }
  new Worker(this, type).salvage(fn);
};

/**
 * Get the job types present and callback `fn(err, types)`.
 *
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.types = function(fn){
  this.client.smembers(redis.keyPrefix + 'job:types', fn);
  return this;
};

/**
 * Return job ids with the given `state`, and callback `fn(err, ids)`.
 *
 * @param {String} state
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.state = function(state, fn){
  this.client.zrange(redis.keyPrefix + 'jobs:' + state, 0, -1, fn);
  return this;
};

/**
 * Get queue work time in milliseconds and invoke `fn(err, ms)`.
 *
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.workTime = function(fn){
  this.client.get(redis.keyPrefix + 'stats:work-time', function(err, n){
    if (err) return fn(err);
    fn(null, parseInt(n, 10));
  });
  return this;
};

/**
 * Get cardinality of `state` and callback `fn(err, n)`.
 *
 * @param {String} state
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.card = function(state, fn){
  this.client.zcard(redis.keyPrefix + 'jobs:' + state, fn);
  return this;
};

/**
 * Completed jobs.
 */

Queue.prototype.complete = function(fn){
  return this.state('complete', fn);
};

/**
 * Failed jobs.
 */

Queue.prototype.failed = function(fn){
  return this.state('failed', fn);
};

/**
 * Inactive jobs (queued).
 */

Queue.prototype.inactive = function(fn){
  return this.state('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.active = function(fn){
  return this.state('active', fn);
};

/**
 * Completed jobs count.
 */

Queue.prototype.completeCount = function(fn){
  return this.card('complete', fn);
};

/**
 * Failed jobs count.
 */

Queue.prototype.failedCount = function(fn){
  return this.card('failed', fn);
};

/**
 * Inactive jobs (queued) count.
 */

Queue.prototype.inactiveCount = function(fn){
  return this.card('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.activeCount = function(fn){
  return this.card('active', fn);
};

/**
 * Delayed jobs.
 */

Queue.prototype.delayedCount = function(fn){
  return this.card('delayed', fn);
};

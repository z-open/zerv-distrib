const assert = require('assert');
const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');
const ioRedisLock = require('ioredis-lock');
const serverStatusService = require('./server-status.service');
const processService = require('./process.service');


const logger = zlog.getLogger('process-monitor/server');

const supportedProcessTypes = {};
let zerv;
const activeProcesses = {};
let waitForProcessingQueue;
const thisServerStart = new Date();

module.exports = {
  setZervDependency,
  monitorQueue,
  addProcessType,
  getServerId: serverStatusService.getServerId
};

function setZervDependency(zervInstance) {
  zerv = zervInstance;
}

/**
 * add the type of process handled by the zerv server and its implementation
 * When process is submitted by the queue, this zerv instance will process it if capacity allows.
 * @param {String} type
 * @param {function} executeFn
 * @param {Object} options
 *   gracePeriodInMins: if the process did not complete in that time, it might have crashed. so give a chance to restart it to this server
 *   wasteTimeInSecs will add a duration to a process execution useful to simulate more concurrency
 */
function addProcessType(type, executeFn, options) {
  let execute;
  if (_.isNumber(options.wasteTimeInSecs) && options.wasteTimeInSecs>0) {
    // delay execution to pretend the process is slow.
    execute = function() {
      return new Promise((resolve, reject) => {
        setTimeout(async () => {
          try {
            resolve(await executeFn.apply(this, arguments));
          } catch (err) {
            reject(err);
          }
        }
        , options.wasteTimeInSecs * 1000);
      });
    };
  } else {
    execute = executeFn;
  }
  supportedProcessTypes[type] = _.assign({ execute }, options);
}


/**
 * This function starts the process queue monitor
 * Processes submitted in the queue will be executed by this server
 * only if it handles the type of process submitted and if it is not already running full capacity.
 *
 * This function also notifies the server status
 * @param {String} serverName  the name or type of server
 * @param {Number} port which the port where it runs
 * @param {Number} capacityProfile the number of processes the server can run at the same time
 */
function monitorQueue(serverName, port, capacityProfile) {
  assert(_.isNumber(capacityProfile) && capacityProfile>=0 && capacityProfile<10000, 'The capacity is invalid');
  // ZERV_STAY_ALIVE is the duration before server is no longer considered alive by the cluster
  serverStatusService.createOne(serverName, process.env.ZERV_STAY_ALIVE || 30);
  // if there is no capacity or process type supported. monitor does NOT listen to the queue
  if (capacityProfile > 0 && _.keys(supportedProcessTypes).length) {
    listenProcessQueueForNewRequests(capacityProfile);
  }
  serverStatusService.notifyServerStatusPeriodically();
}


/**
 * This function locks the access to the queue using redis lock mechanism
 *
 * @returns {Object} with a release function to release the lock.
 */
async function getLock() {
  if (process.env.REDIS_ENABLED !== 'true') {
    return { release: _.noop}; ;
  }

  const processQueueResisLock = ioRedisLock.createLock(zerv.getRedisClient(), {
    timeout: process.env.REDIS_LOCK_TIMEOUT_IN_MS || 20000,
    // retries should be based on how many servers running and how busy is the sytem
    // since many processes will try to acquire the lock at the same time on a busy system
    retries: process.env.REDIS_LOCK_RETRY || 3000,
    delay: process.env.REDIS_LOCK_DELAY_IN_MS || 100,
  });

  try {
    await processQueueResisLock.acquire('zerv:process-queue:lock');
    logger.debug('Process Queue Locked');
    return { release: () => {
      processQueueResisLock.release();
      logger.debug('Process Queue unLocked');
    }
    };
  } catch (err) {
    // might happen when the retries is too low
    logger.error('Redis Locking error: %s', err.message);
    throw new Error('LOCK_ERROR');
  }
}


function listenProcessQueueForNewRequests(capacityProfile) {
  logger.info('Server %b is monitoring process queue for executing processes %b. max capacity: %s', serverStatusService.getServerId(), _.keys(supportedProcessTypes), capacityProfile);

  zerv.onChanges('TENANT_PROCESS_DATA', async (tenantId, process, notificationType) => {
    if (process.status === processService.PROCESS_STATUSES.PENDING) {
      logger.info('Received new process in the queue: [%s/%s]', process.type, process.name);
      scheduleToCheckForNewProcessResquests(capacityProfile);
    }
  });

  setTimeout(() => {
    logger.info('Checking queue for pending processes to execute as per capacity %b...', capacityProfile);
    // when the server starts, it will try to run all tasks in the queue up to its capacity;
    scheduleToCheckForNewProcessResquests(capacityProfile);

    // Safety Check: every now and then,let's recheck the queue.
    // If locking mechanism fails (redis failure for ie), there might be process requests in the queue
    // not handled by any server if there is no activity in the infrastructure.
    //
    setInterval(() => {
      if (!isServerAtFullCapacity(capacityProfile)) {
        logger.debug('Process queue health check for unhandled process requests');
        scheduleToCheckForNewProcessResquests(capacityProfile);
      }
    }, (process.env.ZERV_PROCESS_QUEUE_HEALTH_CHECK_IN_SECS || 60)* 1000);
  }, 5 * 1000);
}


/**
 * Check if there are new processes and the queue
 * If the queue is currently being process, wait for processing again as new requests
 * might have been submitted while system was locking the queue.
 *
 * This prevents from checking multiple times the queue after the queue was read but only once.
 * It is not necessary and would be put extra load on redis to only rely on its lock.
 *
 *
 * @param {*} capacityProfile
 */
function scheduleToCheckForNewProcessResquests(capacityProfile) {
  if (!waitForProcessingQueue) {
    runNextProcesses(capacityProfile);
  } else {
    waitForProcessingQueue.then(() => {
      if (!waitForProcessingQueue) {
        runNextProcesses(capacityProfile);
      }
    });
  }
}

async function runNextProcesses(capacityProfile) {
  let done;
  waitForProcessingQueue = new Promise((resolve) => {
    done = resolve;
  });

  if (isServerShuttingDown()) {
    return;
  }
  const nextProcessesToRun = await selectNextProcessesToRun(serverStatusService.getServerId(), capacityProfile || 1);
  done();
  waitForProcessingQueue = null;

  const runningProcesses = _.map(nextProcessesToRun, async (process) => {
    try {
      activeProcesses[process.id] = process;
      await executeProcess(process, serverStatusService.getServerId());
      delete activeProcesses[process.id];
    } catch (err) {
      delete activeProcesses[process.id];
    }
  });

  if (runningProcesses.length) {
    try {
      await Promise.race(runningProcesses);
    } catch (err) {
      // process seems to have crashed.
    }
    // let's check if there is more processes in the queue that
    // were added but not processed after their request notification by any server due to capacity restriction
    scheduleToCheckForNewProcessResquests(capacityProfile);
  }
}


function isServerShuttingDown() {
  return zerv.isServerShutDownInProgress();
}

/**
 * This function should decide if this server has 0 availability
 * to take any other processes in.
 *
 * @param {Object} capacityProfile  currently it is a number
 */
function isServerAtFullCapacity(capacityProfile) {
  // Server could have a different capacity for long processes
  // otherwise short process might take time before being executed.
  return _.keys(activeProcesses).length >= capacityProfile;
}

async function selectNextProcessesToRun(serverId, capacityProfile) {
  if (isServerAtFullCapacity(capacityProfile)) {
    logger.debug('Server is currently running at full capacity %b and will not accept more processes for now', capacityProfile);
    return;
  }
  // Why a Lock?
  // When the current process is updating the status  to in IN_PROGRESS
  // Another  process/server might be requesting for permission to set the status at the same time for the same entity integration.
  // The integration request will not be able to check the process status until the first process has completed updating the status.
  // the other integration will end up not starting avoiding multiple processes running on the same data.

  // When the transaction is completed, the lock is released.
  // this makes sure no other server will try to run process at the same time
  // other servers are STILL available to run a different sync on other entity/direction to distribute load.
  const processesToExecute = [];
  try {
    const lock = await getLock();

    // the queue returns all records that are not current locked by any other transaction
    const queue = await processService.findAllPendingAndInProgressProcesses(_.keys(supportedProcessTypes));
    // chronological order based on processe created date and status so that stalled processes show up first to be reprocessed asap
    queue.sort((a, b) => {
      if (a.status === processService.PROCESS_STATUSES.IN_PROGRESS && a.status !== b.status) {
        return -1;
      }
      return new Date(a.timestamp.createdDate).getTime() - new Date(b.timestamp.createdDate).getTime();
    });

    logger.debug('Current Queue: ', JSON.stringify(_.map(queue, p => _.pick(p, ['type', 'name', 'status', 'serverId', 'duration'])), null, 3));

    for (const process of queue) {
      if (isServerShuttingDown() || isServerAtFullCapacity(capacityProfile)) {
        break;
      }
      if ((process.status === processService.PROCESS_STATUSES.PENDING || checkIfProcessIsStalled(serverId, process)) &&
          canServerExecuteProcessNow(process, capacityProfile)) {
        process.status = processService.PROCESS_STATUSES.IN_PROGRESS;
        process.start = new Date();
        process.end = null;
        process.serverId = serverId;
        process.progressDescription = `Server ${serverId} will execute this process`;
        activeProcesses[process.id] = process;
        await processService.updateOne(process.tenantId, process);
        processesToExecute.push(process);
      }
    }

    await lock.release();
  } catch (err) {
    logger.error('Error when selecting next process to run. %s', err.message, err.stack || err);
    // some processes might be able to execute;
  }
  return processesToExecute;
}

/**
 * This function was designed to handle the capacity profile
 * A process might be executed based on rules
 * Ex: If system is working on high priority task, logic should be defined to still allow some low priority tasks to execute
 * if they have been seating for a while.
 *
 * Or decision could be made on the status of all servers.
 *
 * The capacity profile should contain those rules that will be executed in this function
 *
 * @param {Process} process
 * @param {Object} capacityProfile
 */
function canServerExecuteProcessNow(process, capacityProfile) {
  // since server is not running at full capactity, and nothing prevents this process from running...
  return true;
}


/**
 *
 * Check if the process is not stalled. It means that it does not remain in progress forever.
 * This could happen if the process crashes with incorrect error handling (should not happen) or that the server that handle the process has crashed or rebooted.
 *
 * Note:
 * Currently a grace period is used to define is the process is valid.
 * but a true solution would be to detect if a server is no longer alive - Which would mean the process will never complete and should be put back to pending.
 *
 *
 * @param {String} serverId
 * @param {TenantProcess} process
 *
 * @returns {boolean} true if the process is stalled
 */
function checkIfProcessIsStalled(serverId, process) {
  if (process.status !== processService.PROCESS_STATUSES.IN_PROGRESS) {
    return false;
  }
  // Only pulling data from an external system can take substantial time. (ex pulling all projects or timesheets in idb)
  // the following grace period gives enough time to complete the fetch, without misleading the integration to believe the process has crashed and restart a same process (same entity, same direction) while the previous one has not completed.
  const gracePeriodInMinutes = supportedProcessTypes[process.type].gracePeriodInMins || (2 * 60); // hours
  const currentProcessDuration = moment.duration(process.duration, 'seconds'); ;
  logger.debug('%s: This process has been running on %b for %s.', process.display, process.serverId, currentProcessDuration.humanize());

  const serverOwner = serverStatusService.findByServerId(process.serverId);

  // should based on last process update instead (unfortunately no all processes are updated while running)
  if (currentProcessDuration.asMinutes() > gracePeriodInMinutes) {
    logger.warn('The current process %b on %b seems stalled. gracePeriodInMinutes is %b. Server/Process might have crashed or interrupted. Let it restart.', process.display, process.serverId, moment.duration(gracePeriodInMinutes, 'minutes').humanize());
    return true;
  }

  if (!serverOwner) {
    if (moment.duration(moment().diff(thisServerStart)).asSeconds() > 120) {
      // usually servers start quickly and notify their presence.
      logger.warn('The current process %b on %b is stalled. The server %b which started the process is offline.', process.display, process.serverId);
      return true;
    }
  } else if (!moment(serverOwner.start).isBefore(process.start)) {
    logger.warn('The current process %b on %b is stalled. The server %b which started the process was rebooted.', process.display, process.serverId);
    return true;
  } else if (!serverOwner.isAlive()) {
    logger.warn('The current process %b on %b is stalled. The server %b which started the process is offline.', process.display, process.serverId);
    return true;
  }

  return false;
}


/**
 * Execute the process implementation and update its status thru the execution
 * @param {TenantProcess} process
 * @param {String} byServer
 */
async function executeProcess(process, byServer) {
  const supportedProcessType = supportedProcessTypes[process.type];
  const executeFn = supportedProcessType.execute;
  process.status = processService.PROCESS_STATUSES.IN_PROGRESS;
  process.progressDescription = 'Started';
  process.serverId = byServer;
  process.start = new Date();
  process.end = null;

  // activity is an object informing that the server is currently running
  // that must be awaited if the server is shutting down
  // it is also an handle on the process to update its progress
  const processHandle = await zerv.registerNewActivity(process.type, {tenantId: process.tenantId}, {origin: 'zerv distrib'});
  processHandle.setProgressDescription = setProgressDescription;
  try {
    await setProgressDescription(`Started by server ${byServer}`);

    const result = await executeFn(process.tenantId, processHandle, process.params);
    process.status = processService.PROCESS_STATUSES.COMPLETE;
    process.progressDescription = result.description;
    // this amount of data will be notified to the entire cluster. Careful!
    // Later on, pass a dataId that would help locate the result in a temporary location as SalesForce does.
    process.data = result.data;
    process.end = new Date();
    processHandle.done();
    logger.debug('%s: Completed successfully after %s seconds. Result: %s', process.display, process.duration, process.progressDescription);
  } catch (err) {
    process.status = processService.PROCESS_STATUSES.ERROR;
    process.error = _.isError(err) ? {message: err.message, description: err.description} : err;
    process.end = new Date();
    processHandle.fail(err);
    logger.error('%s: Failure after %s seconds - %s', process.display, process.duration, err.description || err.message, err.stack || err);
  }
  await processService.updateOne(process.tenantId, process);

  async function setProgressDescription(text) {
    process.progressDescription = text;
    logger.info('%s: %s', process.display, text);
    await processService.updateOne(process.tenantId, process);
  }
}

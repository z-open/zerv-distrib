const assert = require('assert');
const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');
const ip = require('ip');
const IoRedis = require('ioredis');
const ioRedisLock = require('ioredis-lock');
const serverStatusService = require('./server-status.service');

const logger = zlog.getLogger('process-monitor/server');

const supportedProcessTypes = {};
let processService, zerv;
let serverUniqId = 'UndefinedServerId';
const activeProcesses = {};
let redisClient;

module.exports = {
  setProcessService,
  monitorQueue,
  addProcessType,
  getServerId,
};

function setProcessService(impl, zervInstance) {
  processService = impl;
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

function getServerId() {
  return serverUniqId;
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
  capacityProfile = 2;
  assert(_.isNumber(capacityProfile) && capacityProfile>=0 && capacityProfile<10000, 'The capacity is invalid');
  serverUniqId = `${serverName}/${ip.address()}:${port}`;

  // ZERV_STAY_ALIVE is the duration before server is no longer considered alive by the cluster
  serverStatusService.createOne(serverUniqId, process.env.ZERV_STAY_ALIVE || 30);

  // if there is no capacity or process type supported. monitor does NOT listen to the queue
  if (capacityProfile > 0 && _.keys(supportedProcessTypes).length) {
    listenProcessQueueForNewRequests(capacityProfile);
  }
  notifyServerStatusPeriodically();
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
  if (!redisClient) {
    // --------------------------
    // Later on, let's use instead the redis client initalized in zerv-sync/clustering
    // if there were any error, zerv-sync will throw errors anyway
    const connectionParams = {
      port: process.env.REDIS_PORT || 6379,
      host: process.env.REDIS_HOST || '127.0.0.1',
    };
    redisClient = new IoRedis(connectionParams);
    // --------------------------
  }
  const processQueueResisLock = ioRedisLock.createLock(redisClient, {
    timeout: process.env.REDIS_LOCK_TIMEOUT_IN_MS || 20000,
    // retries should be based on how many servers running and how busy is the sytem
    // since many processes will try to acquire the lock at the same time on a busy system
    retries: process.env.REDIS_LOCK_RETRY || 3000, 
    delay: process.env.REDIS_LOCK_DELAY_IN_MS || 100,
  });

  try {
    await processQueueResisLock.acquire('zerv:process-queue:lock');
    logger.warn('Process Queue Locked');
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
  logger.info('Server %b is monitoring process queue for executing processes %b. max capacity: %s', serverUniqId, _.keys(supportedProcessTypes), capacityProfile);

  zerv.onChanges('TENANT_PROCESS_DATA', async (tenantId, process, notificationType) => {
    if (process.status === processService.PROCESS_STATUSES.PENDING) {
      logger.info('Received new process in the queue: [%s/%s]', process.type, process.name);
      runNextProcesses(capacityProfile);
    }
  });

  setTimeout(() => {
    logger.info('Start checking queue for pending processes to execute as per capacity %b...', capacityProfile);
    // when the server starts, it will try to run all tasks in the queue up to its capacity;
    runNextProcesses(capacityProfile);

    // Safety Check: every now and then,let's recheck the queue.
    // If locking mechanism fails (redis failure for ie), there might be process requests in the queue
    // not handled by any server if there is no activity in the infrastructure.
    //
    setInterval(() => {
      if (!isServerAtFullCapacity(capacityProfile)) {
        logger.info('Process queue health check for unhandled process requests');
        runNextProcesses(capacityProfile);
      }
    }, (process.env.ZERV_PROCESS_QUEUE_HEALTH_CHECK_IN_SECS || 60)* 1000);
  }, 5 * 1000);
}


function notifyServerStatusPeriodically(thisServerStatus) {
  setInterval(() => {
    const serverStatus = serverStatusService.findByServerId(getServerId());
    serverStatus.activeProcesses= zerv.getActivitiesInProcess();
    serverStatus.userSessions = zerv.getLocalUserSessions();
    serverStatusService.updateOne(serverStatus);
  }, serverStatusService.findByServerId(getServerId()).timeout*1000/2);
}


async function runNextProcesses(capacityProfile) {
  let done = false;
  while (!done) {
    if (isServerShuttingDown()) {
      return;
    }
    const nextProcessesToRun = await selectNextProcessesToRun(serverUniqId, capacityProfile || 1);

    const runningProcesses = _.map(nextProcessesToRun, async (process) => {
      try {
        activeProcesses[process.id] = process;
        await executeProcess(process, serverUniqId);
        delete activeProcesses[process.id];
      } catch (err) {
        delete activeProcesses[process.id];
      }
    });

    if (!runningProcesses.length) {
      // there is no process to execute at this time
      // notification will trigger if any new
      done = true;
    } else {
      try {
        await Promise.race(runningProcesses);
      } catch (err) {
        // process seems to have crashed.
      }
      // let's check if there is more processes in the queue that
      // were added but not processed after their request notification by any server due to capacity restriction
      done = false;
    }
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
    logger.info('Server is currently running at full capacity %b and will not accept more processes for now', capacityProfile);
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
    await zerv.startTransaction({ name: 'selectNextProcessToRun on ' + serverId })
        .execute(async (transaction) => {
          // the queue returns all records that are not current locked by any other transaction
          const queue = await processService.findAllPendingAndInProgressProcesses(transaction, _.keys(supportedProcessTypes));
          logger.debug('Current Queue: ', JSON.stringify(_.map(queue, p => _.pick(p, ['type', 'name', 'status', 'serverId', 'duration'])), null, 3));

          for (const process of queue) {
            if (isServerShuttingDown()) {
              break;
            }
            if (process.status === processService.PROCESS_STATUSES.PENDING || checkIfProcessIsStalled(serverId, process)) {
              if (canServerExecuteProcessNow(process, capacityProfile)) {
                process.status = processService.PROCESS_STATUSES.IN_PROGRESS;
                process.start = new Date();
                process.end = null;
                process.serverId = serverId;
                process.progressDescription = `Server ${serverId} will execute this process`;
                activeProcesses[process.id] = process;
                await processService.updateOne(process.tenantId, process, { transaction });
                processesToExecute.push(process);
              }
            }
          }
        });
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
  if (isServerAtFullCapacity(capacityProfile)) {
    logger.info('Server is now running at full capacity %b and will not accept more processes.', capacityProfile);
    return false;
  }
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
  logger.warn('%s: This process has been running on %b for %s.', process.display, process.serverId, currentProcessDuration.humanize());

  const serverOwner = serverStatusService.findByServerId(process.serverId);

  const isCurrentProcessRunningWithinAcceptableTimeFrame =
        // - if the process crashes, it will allow restart after the grace period.
        // but in theory, all errors are cached, so the process will always completes one way or another. This could be needed in a cluster as a simple way to unlock crashed processes.
        currentProcessDuration.asMinutes() < gracePeriodInMinutes &&
        // - if the serverOwner was rebooted it means the process is stalled, and can be restarted by any server
        // ex: if it was stopped and restarted during a sync process (ex for code upgrade), integration - which started the process -- will be able to restart right away without taking in consideration the grace period.
        // - if the server owner is never restarted (never notifies that it is alive), the grace period will also release the process.
        (!serverOwner || moment(serverOwner.start).isBefore(process.start));

  if (isCurrentProcessRunningWithinAcceptableTimeFrame) {
    logger.warn('Let\'s wait for completion %b on %b. gracePeriodInMinutes is %s.', process.display, process.serverId, moment.duration(gracePeriodInMinutes, 'minutes').humanize());
    return false;
  }
  logger.warn('The current process %b on %b seems stall. gracePeriodInMinutes is %b. Server/Process might have crashed or interrupted. Let it restart.', process.display, process.serverId, moment.duration(gracePeriodInMinutes, 'minutes').humanize());
  return true;
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
    process.error = err;
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

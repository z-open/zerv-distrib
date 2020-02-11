const assert = require('assert');
const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');
const ip = require('ip');
const serverStatusService = require('./server-status.service');

const logger = zlog.getLogger('process-monitor/server');

const supportedProcessTypes = {};
let processService, zerv;
let serverUniqId = 'UndefinedServerId';
const activeProcesses = {};

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
 * @param {Number} capacityProfile the number of processes it can run at the same time
 */
function monitorQueue(serverName, port, capacityProfile) {
  assert(capacityProfile, 'The profile must be provided');
  assert(capacity>=1 && capacityProfile<10000, 'The capacity seems invalid');
  serverUniqId = `${serverName}/${ip.address()}:${port}`;

  // ZERV_STAY_ALIVE is the duration before server is no longer considered alive by the cluster
  serverStatusService.createOne(serverUniqId, process.env.ZERV_STAY_ALIVE || 30);

  logger.info('Server %b is monitoring process queue for executing processes %b. max capacity: %s', serverUniqId, _.keys(supportedProcessTypes), capacityProfile);
  zerv.onChanges('TENANT_PROCESS_DATA', (tenantId, process, notificationType) => {
    if (process.status === processService.PROCESS_STATUSES.PENDING) {
      runNextProcess(capacityProfile);
    }
  });

  notifyServerStatusPeriodically();

  setTimeout(() => {
    startProcessesInQueueUpToServerCapacity(capacityProfile);
  }, 10 * 1000);
}

async function startProcessesInQueueUpToServerCapacity(capacityProfile) {
  logger.info('Checking queue for pending processes to execute as per capacity %b...', capacityProfile);
  // when the server starts, it will try to run all tasks in the queue up to its capacity;
  let n = capacityProfile;
  while (n--) {
    runNextProcess(capacityProfile);
  };
}

function notifyServerStatusPeriodically(thisServerStatus) {
  setInterval(() => {
    const serverStatus = serverStatusService.findByServerId(getServerId());
    serverStatus.activeProcesses= zerv.getActivitiesInProcess();
    serverStatus.userSessions = zerv.getLocalUserSessions();
    serverStatusService.updateOne(serverStatus);
  }, serverStatusService.findByServerId(getServerId()).timeout*1000/2);
}


async function runNextProcess(capacityProfile) {
  let nextProcessToRun;
  do {
    if (isServerShuttingDown()) {
      return;
    }
    nextProcessToRun = await selectNextProcessToRun(serverUniqId, capacityProfile || 1);
    if (!_.isNil(nextProcessToRun)) {
      try {
        activeProcesses[nextProcessToRun.id] = nextProcessToRun;
        await executeProcess(nextProcessToRun, serverUniqId);
        delete activeProcesses[nextProcessToRun.id];
      } catch (err) {
        delete activeProcesses[nextProcessToRun.id];
      }
    }
  }
  // While the last process was executed if any,
  // there mmight be new processes pending in the queue that have not been handled yet by any server
  // due to server capacity limit.
  // then only, let's loop to check the queue.
  // otherwise loop is terminated, new processes will be executed when notified.
  while (!_.isNil(nextProcessToRun));
}

function isServerShuttingDown() {
  return zerv.isServerShutDownInProgress();
}

function isServerAtFullCapacity(capacityProfile) {
  // Server could have a different capacity for long processes
  // otherwise short process might take time before being executed.
  return _.keys(activeProcesses).length >= capacityProfile;
}

async function selectNextProcessToRun(serverId, capacityProfile) {
  // Why a transaction?
  // When the current process is updating the status  to in IN_PROGRESS
  // Another  process/server might be requesting for permission to set the status at the same time for the same entity integration.
  // The integration request will not be able to check the process status until the first process has completed updating the status.
  // the other integration will end up not starting avoiding multiple processes running on the same data.

  // When the transaction is completed, the lock is released.
  // this makes sure no other server will try to run process at the same time
  // other servers are STILL available to run a different sync on other entity/direction to distribute load.

  return zerv.startTransaction({ name: 'selectNextProcessToRun on ' + serverId })
      .execute(async (transaction) => {
        // the queue returns all records that are not current locked by any other transaction
        const queue = await processService.findAllPendingAndInProgressProcesses(transaction);
        if (isServerAtFullCapacity(capacityProfile)) {
          logger.info('Server is running at full capacity %b and will not accept more processes for now', capacityProfile);
          return;
        }
        logger.debug('Current Queue: ', JSON.stringify(_.map(queue, p => _.pick(p, ['type', 'name', 'status', 'serverId', 'duration'])), null, 3));

        let processToExecute;
        for (const process of queue) {
          const processImplementation = supportedProcessTypes[process.type];
          if (!_.isNil(processImplementation) && ( process.status === processService.PROCESS_STATUSES.PENDING || checkIfProcessIsStalled(serverId, process))) {
            process.status = processService.PROCESS_STATUSES.IN_PROGRESS;
            process.start = new Date();
            process.end = null;
            process.serverId = serverId;
            process.progressDescription = `Server ${serverId} will execute this process`;
            await processService.updateOne(process.tenantId, process, { transaction });
            processToExecute = process;
            break;
          }
        }
        return processToExecute;
      });
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
  process.progressDescription = `Started by server ${byServer}`;

  // activity is an object informing that the server is currently running
  // that must be awaited if the server is shutting down
  // it is also an handle on the process to update its progress
  const processHandle = await zerv.registerNewActivity(process.type, {tenantId: process.tenantId}, {origin: 'zerv distrib'});
  processHandle.setProgressDescription = setProgressDescription;
  try {
    await processService.updateOne(process.tenantId, process);

    const result = await executeFn(process.tenantId, processHandle, process.params);
    process.status = processService.PROCESS_STATUSES.COMPLETE;
    process.progressDescription = result.description;
    // this amount of data will be notified to the entire cluster. Careful!
    // Later on, pass a dataId that would help locate the result in a temporary location as SalesForce does.
    process.data = result.data;
    process.end = new Date();
    processHandle.done();
    logger.debug('%s: Completed successfully after %s seconds', process.display, process.duration);
  } catch (err) {
    process.status = processService.PROCESS_STATUSES.ERROR;
    process.error = err;
    process.end = new Date();
    processHandle.fail(err);
    logger.error('%s: Failure after %s seconds - %s', process.display, process.duration, err.description || err.message, err.stack || err);
  }
  await processService.updateOne(process.tenantId, process);

  function setProgressDescription(text) {
    process.progressDescription = text;
    logger.info('%s: %s', process.display, text);
    processService.updateOne(process.tenantId, process);
  }
}

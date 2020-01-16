const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');
let processService, zerv;

const logger = zlog.getLogger('zerv/process-monitor/client');

module.exports = {
  setProcessService,
  submitProcess,
  waitForCompletion,
};


function setProcessService(impl, zervInstance) {
  processService = impl;
  zerv = zervInstance;
  listenToTenantProcessData();
}

/**
 * if a process monitor client call is waiting for a process to complete
 * the process will be in the waiting queue.
 * When the process completion is notified, the promise is the queue will be resolved and the call will be answered.
 */
const waitQueue = {};


function listenToTenantProcessData() {
  /**
     * When tenant process data is notified, All listening servers will receive the notification
     * and will be able to resolve the promises in the wait queue.
     */
  zerv.onChanges('TENANT_PROCESS_DATA', (tenantId, process, notificationType) => {
    const awaitedProcessHandler = waitQueue[process.id];
    if (awaitedProcessHandler && notificationType==='UPDATE') {
      if (process.status === 'complete') {
        return awaitedProcessHandler.resolve(new processService.TenantProcess(process));
      } else if (process.status === 'error') {
        return awaitedProcessHandler.reject(new processService.TenantProcess(process));
      }
    }
  });
}

/**
 * Submit a new process into the queue for one server of the cluster to process.
 * If a similar process already exists, it is returned.
 *
 * Note
 * ----
 * a browser submits via this client a new process and waits for its completion
 * if it is refreshed
 * the browser might try to submit the same process
 * but this function will return the same process instead of spawning a new one, even though the browser might have connected a different server via its socket.
 * finally the browser will get the response way faster without any load to the server.
 *
 * @param {String} tenantId
 * @param {String} type
 * @param {String} name
 * @param {Object} params that will be passed to the process monitor server when it executes the implementation of this process.
 * @param {Object} options
 *    @property {Boolean} single when true, only one process with this type and name will run in the cluster. Default true.
 *
 * @returns {TenantProcess} the process that was started or the existing process
 */
async function submitProcess(tenantId, type, name, params, options = {}) {
  const existingActiveProcess = await processService.findOneByTenantIdAndTypeAndNameAndInStatusList(tenantId, type, name, [processService.PROCESS_STATUSES.PENDING, processService.PROCESS_STATUSES.IN_PROGRESS]);

  if (existingActiveProcess && existingActiveProcess.single) {
    // Only one process runs enterprise wide with this exact tenantId, type and name
    // to prevent spawning the same type of process and consume too many resources.
    logger.info('%s: process already exists and has been %b on %b for %s.', existingActiveProcess.display, existingActiveProcess.status, existingActiveProcess.serverId || 'queue', moment.duration(existingActiveProcess.duration, 'seconds').humanize());
    return existingActiveProcess;
  }
  const process = new processService.TenantProcess({
    tenantId,
    type,
    name,
    params,
    server: null,
    status: processService.PROCESS_STATUSES.PENDING,
    single: _.isNil(options.single) ? true : options.single // by default only one process with this type and name can run in the cluster at a time
  });
  logger.info('%s: Submitted new process into the queue.', process.display);
  await processService.createOne(tenantId, process);
  return process;
}

/**
 * Wait for the completion of a process that is run on a server of the cluster.
 * The server running the process is the one that picked up the process in the queue.
 * It will notify the client waiting for its completion when the process completed.
 *
 * @param {TenantProcess} process
 * // not implemented yet: timeout
 *
 * @returns {Promise} resolve with the data or reject with the error
 */
function waitForCompletion(process) {
  logger.info('%s: Waiting for completion...', process.display);
  let awaitedProcessHandler = waitQueue[process.id];
  if (awaitedProcessHandler) {
    return awaitedProcessHandler.promise;
  }
  awaitedProcessHandler = {process};
  const promise = new Promise((resolve, reject) => {
    // when the server executing the process completes, it will notify the process.
    // This server is listening to process notifications
    // It will resolve and reject accordingly if the process is in its waitqueue.
    awaitedProcessHandler.resolve = (process) => {
      logger.info('%s: Completed - %s', process.display, process.progressDescription);
      delete waitQueue[awaitedProcessHandler.process.id];
      resolve(process.data);
    };
    awaitedProcessHandler.reject = (process) => {
      logger.info('%s: Error', process.display, process.error);
      delete waitQueue[awaitedProcessHandler.process.id];
      reject(process.error);
    };
    // should timeout if process is taking too long.
  });
  awaitedProcessHandler.promise = promise;
  waitQueue[process.id] = awaitedProcessHandler;
  return awaitedProcessHandler.promise;
}

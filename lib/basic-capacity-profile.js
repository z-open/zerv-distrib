const _ = require('lodash');
const assert = require('assert');
const processService = require('./process.service');

class BasicCapacityProfile {
  constructor(maxProcesses) {
    this.maxProcesses = maxProcesses;
    assert(maxProcesses>=0 && maxProcesses<10000, 'The capacity is invalid');
  }

  setActiveProcesses(activeProcesses) {
    this.activeProcesses = activeProcesses;
  }

  setSupportedProcessType(supportedProcessTypes) {
    this.supportedProcessTypes = supportedProcessTypes;
  }

  toString() {
    return this.maxProcesses;
  }

  /**
     * This function should decide if this server has 0 availability
     * to take any other processes in.
     *
     */
  isServerAtFullCapacity() {
    // Server could have a different capacity for long processes
    // otherwise short process might take time before being executed.
    return _.keys(this.activeProcesses).length >= this.maxProcesses;
  }


  /**
   * order the processes in the process queue
   * by priority and chronologically
   *
   * @param {Array<TenantProcess} processQueue
   */
  orderQueue(processQueue) {
    processQueue.sort((processA, processB) => {
      if (processA.status === processService.PROCESS_STATUSES.IN_PROGRESS && processA.status !== processB.status) {
        return -1;
      }
      if (getPriority(processA, this.supportedProcessTypes) < getPriority(processB, this.supportedProcessTypes)) {
        return -1;
      }
      return processA.createdDate.getTime() - processB.createdDate.getTime();
    });
  }
}

function getPriority(process, supportedProcessTypes) {
  const priority = _.get(supportedProcessTypes, process.type + '.priority', 10);
  if (!_.isNumber(priority)) {
    throw new Error(`Priority in supported process type ${process.type} must be a number`);
  }
  return priority;
}


module.exports = BasicCapacityProfile;

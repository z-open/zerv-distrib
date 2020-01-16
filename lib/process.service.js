const
  _ = require('lodash'),
  UUID = require('uuid'),
  zlog = require('zlog4js');
const moment = require('moment');

const logger = zlog.getLogger('zerv/distrib/processService');

const PROCESS_STATUSES = {
  IN_PROGRESS: 'in progress',
  COMPLETE: 'complete',
  ERROR: 'error',
  PENDING: 'pending'
};


class TenantProcess {
  constructor(obj) {
    this.id = obj.id || UUID.v4();
    this.timestamp = obj.timestamp || {};
    this.type = obj.type || null;
    this.name = obj.name || null;
    this.tenantId = obj.tenantId || null;
    this.params = obj.params || {};
    this.single = obj.single || false; // When true only one process with this type and name can run in the cluster at a time

    this.revision = obj.revision || 0;

    this.start = obj.start || null;
    this.end = obj.end || null;
    this.status = obj.status || PROCESS_STATUSES.PENDING;
    this.progressDescription = obj.progressDescription || null;
    this.serverId = obj.serverId || null;

    this.data = obj.data || null;
    this.error = obj.error || null;
  }
  get display() {
    return `Process ${this.id} [${this.type}/${this.name}] for tenant ${this.tenantId}`;
  }

  get duration() {
    let duration;
    if (this.status !== PROCESS_STATUSES.IN_PROGRESS) {
      duration = moment.duration(moment(this.end).diff(this.start));
    } else {
      duration = moment.duration(moment().diff(this.start));
    }
    return duration.asSeconds();
  }
}


module.exports = {
  setProcessService,
  purgeQueue,
  createOne,
  updateOne,
  findOneByTenantIdAndProcessId,
  findOneByTenantIdAndTypeAndNameAndInStatusList,
  findAllPendingAndInProgressProcesses,
  findAll,
  TenantProcess,
  PROCESS_STATUSES,
};

let processServiceImpl, zerv;

function setProcessService(impl, zervInstance) {
  processServiceImpl = impl;
  zerv = zervInstance;
}

/**
 * this retuns all processes that are pending or in progress
 * There is SKIP LOCKED, so if another transaction is already dealing with some records, the query won't return them
 * to prevent dead lock timeouts as many transaction would then wait and timeout.
 * @param {ZervTransaction} transaction
 */
async function findAllPendingAndInProgressProcesses(transaction) {
  const data = await processServiceImpl.findAllInStatusList(transaction, [PROCESS_STATUSES.PENDING, PROCESS_STATUSES.IN_PROGRESS]);
  return _.map(data, p => new TenantProcess(p));

  // // notice the row lock
  // // https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
  // const queryObj = {
  //     dbQueryString: `SELECT doc FROM tenant_process_queue where doc->>\'status\' in ('${PROCESS_STATUSES.PENDING}','${PROCESS_STATUSES.IN_PROGRESS}') FOR UPDATE SKIP LOCKED`,
  //     objectClass: TenantProcess
  // };
  // return transaction.find(queryObj, null, null);
}


async function findAll(max) {
  const data = await processServiceImpl.findAll(max);
  return _.map(data, p => new TenantProcess(p));

  // const queryObj = {
  //     dbQueryString: `SELECT doc FROM tenant_process_queue`,
  //     objectClass: TenantProcess
  // };
  // return dataAccess.find(queryObj, null, null);
}

async function findOneByTenantIdAndTypeAndNameAndInStatusList(tenantId, type, name, statuses) {
  const p = await processServiceImpl.findOneByTenantIdAndTypeAndNameAndInStatusList(tenantId, type, name, statuses);
  return _.isEmpty(p) ? null :new TenantProcess(p);

  // notice the row lock
  // https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
  // so that the caller waits for any other transaction to complete before retrieving the data
  // const statusList = `'` + statuses.join(`','`) + `'`;

  // const queryObj = {
  //     dbQueryString: `SELECT doc FROM tenant_process_queue WHERE tenant_id=$1 AND doc->>\'type\'=$2 AND doc->>\'name\'=$3 AND doc->>\'status\' in (${statusList}) FOR UPDATE`,
  //     objectClass: TenantProcess,
  //     dbQueryParameters: [tenantId, type, name]

  // };
  // return dataAccess.findOne(queryObj, null);
}


function purgeQueue(serverId, oldProcessesIds) {
  return processServiceImpl.purgeQueue(serverId, oldProcessesIds);

  // let's remove old processs from the queue
  // keeping them a bit after completion helps with analyzing server activity.
  // const idsAsString = `'` + oldProcessesIds.join(`','`) + `'`;

  // const queryObj = {
  //     dbQueryString: `DELETE FROM tenant_process_queue where id in (${idsAsString})`,// where doc->>'serverId' = $1`,
  //     //  dbQueryParameters: [serverId],
  // };
  // return dataAccess.executeQuery(queryObj, null);
}

async function createOne(tenantId, process) {
  process.timestamp.lastModifiedDate = new Date();
  process.timestamp.createdDate = new Date();

  await processServiceImpl.createOne(tenantId, process);

  // const queryObj = {
  //     dbQueryString: 'insert into tenant_process_queue (id, tenant_id, doc) values ($1, $2, $3)',
  //     dbQueryParameters: [process.id, tenantId, process]
  // };
  // await dataAccess.executeQuery(queryObj);

  logger.info('Added process %b/%b to processing queue', process.type, process.name);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyCreation(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});

  return process;
}

async function updateOne(tenantId, process, options) {
  process.revision++;
  process.timestamp.lastModifiedDate = new Date();

  await processServiceImpl.updateOne(tenantId, process, options);
  // const queryObj = {
  //     dbQueryString: 'update tenant_process_queue set doc = $2 where id = $1',
  //     dbQueryParameters: [process.id, process]
  // };
  // await _.get(options, 'transaction', dataAccess).executeQuery(queryObj);

  logger.info('Updated process %b/%b in processing queue: %b %s', process.type, process.name, process.status, process.progressDescription);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyUpdate(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function findOneByTenantIdAndProcessId(tenantTransaction, tenantId, processId) {
  const p = await processServiceImpl.indOneByTenantIdAndProcessId(tenantTransaction, tenantId, processId);
  return _.isEmpty(p) ? null :new TenantProcess(p);

  // Notice the row lock preventing access to this information until it is updated or transaction is completed.
  // const queryObj = {
  //     dbQueryString: 'SELECT doc FROM tenant_process_queue WHERE tenant_id = $1 AND id = $2 FOR UPDATE',
  //     dbQueryParameters: [tenantId, processId],
  //     objectClass: TenantProcess
  // };
  // return tenantTransaction.findOne(queryObj);
}

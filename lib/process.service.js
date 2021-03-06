const
  _ = require('lodash'),
  UUID = require('uuid'),
  zlog = require('zlog4js');
const moment = require('moment');

const ACTIVE_QUEUE = 'zerv-active-queue';
const INACTIVE_QUEUE = 'zerv-inactive-queue';
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
  setZervDependency,
  purgeQueue,
  createOne,
  updateOne,
  findOneByTenantIdAndTypeAndNameAndInProgressOrPending,
  findAllPendingAndInProgressProcesses,
  findAll,
  TenantProcess,
  PROCESS_STATUSES,
};

let zerv;

function setZervDependency(zervInstance) {
  zerv = zervInstance;
}


async function findAllPendingAndInProgressProcesses(processTypes) {
  const actives = await zerv.getRedisClient().hvals(ACTIVE_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAll() {
  const max = process.env.ZERV_QUEUE_MAX || 1000;
  const actives = await zerv.getRedisClient().hvals(ACTIVE_QUEUE);
  let inactiveCount = await zerv.getRedisClient().llen(INACTIVE_QUEUE);
  const toRemove = [];
  while (max<inactiveCount--) {
    toRemove.push(['lpop', INACTIVE_QUEUE]);
  }
  await zerv.getRedisClient().pipeline(toRemove).exec();
  const inactives = await zerv.getRedisClient().lrange(INACTIVE_QUEUE, 0, -1);
  const data = _.concat(actives, inactives);
  return _.map(data, p => new TenantProcess(JSON.parse(p)));
}

async function findOneByTenantIdAndTypeAndNameAndInProgressOrPending(tenantId, type, name) {
  const actives =await findAllPendingAndInProgressProcesses([type]);
  const result = _.find(actives, (process) => process.name === name);
  return result;
}

function purgeQueue(serverId, oldProcessesIds) {
  return;
}

async function createOne(tenantId, process) {
  process.timestamp.lastModifiedDate = new Date();
  process.timestamp.createdDate = new Date();

  await zerv.getRedisClient().hset(ACTIVE_QUEUE, process.id, JSON.stringify(process));

  logger.info('Added process %b/%b to processing queue', process.type, process.name);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyCreation(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});

  return process;
}

async function updateOne(tenantId, process) {
  process.revision++;
  process.timestamp.lastModifiedDate = new Date();

  if (process.status === PROCESS_STATUSES.PENDING || process.status === PROCESS_STATUSES.IN_PROGRESS) {
    await zerv.getRedisClient().hset(ACTIVE_QUEUE, process.id, JSON.stringify(process));
  } else {
    await zerv.getRedisClient().hdel(ACTIVE_QUEUE, process.id);
    await zerv.getRedisClient().rpush(INACTIVE_QUEUE, JSON.stringify(process));
  }

  logger.info('Updated process %b/%b in processing queue: %b %s', process.type, process.name, process.status, process.progressDescription);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyUpdate(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

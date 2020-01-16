
const _ = require('lodash');
const moment = require('moment');

const serverStatuses = {};

module.exports = {
  listenToServerStatusData,
  createOne,
  updateOne,
  findAll,
  findByServerId
};

class ServerStatus {
  constructor(obj) {
    this.id = obj.id;
    this.alive = obj.alive;
    this.start = obj.start;
    this.revision = obj.revision;
    this.activeProcesses = obj.activeProcesses || [];
    this.timeout = obj.timeout || 1;
  }

  isAlive() {
    return moment.duration(moment().diff(this.alive)).asMinutes() < this.timeout;
  }
}

let zerv;

function listenToServerStatusData(zervInstance) {
  zerv = zervInstance;
  zerv.onChanges('SERVER_STATUS_DATA', (tenantId, serverStatus, notificationType) => {
    serverStatuses[serverStatus.id] = new ServerStatus(serverStatus);
  });
}

function createOne(serverUniqId, timeout) {
  const serverStatus = new ServerStatus({
    id: serverUniqId,
    alive: new Date(),
    revision: Date.now(),
    start: new Date(),
    timeout
  });
  serverStatuses[serverUniqId] = serverStatus;
  zerv.notifyCreation('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});

  return serverStatus;
}

function updateOne(serverStatus, activeProcesses) {
  serverStatus.revision ++;
  serverStatus.alive = new Date();
  serverStatus.activeProcesses =activeProcesses;
  zerv.notifyUpdate('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});
}

function findAll() {
  return _.values(serverStatuses);
}

function findByServerId(serverId) {
  return serverStatuses[serverId];
}

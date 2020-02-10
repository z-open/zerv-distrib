
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
    this.timeout = obj.timeout || 30;
    this.state = obj.state || 'up and running';
    this.userSessions = obj.userSessions || [];
  }

  isAlive() {
    return moment.duration(moment().diff(this.alive)).asSeconds() < this.timeout;
  }

  getStateDisplay() {
    if (this.isAlive()) {
      return this.state;
    } else {
      return 'offline';
    }
  }
}

let zerv;

function listenToServerStatusData(zervInstance) {
  zerv = zervInstance;
  // all servers are aware of the status of all other servers
  // this could be useful to optimize server capacity later on.
  zerv.onChanges('SERVER_STATUS_DATA', (tenantId, serverStatus, notificationType) => {
    serverStatuses[serverStatus.id] = new ServerStatus(serverStatus);
  });

  zerv.onChanges('SERVER_SHUTDOWN', (tenantId, server, notificationType) => {
    if (server.serverId === zervInstance.getServerId()) {
      const serverStatus = findByServerId(server.serverId);
      if (serverStatus.state !== 'request shutdown') {
        serverStatus.state = 'request shutdown';
        updateOne(serverStatus);
        // stop and shutdown
        zervInstance.stopLocalServer(1, 1);
      }
    }
  });
}

/**
 * create and notify a server status object to the whole zerv cluster
 * It is useful for notifying the current load of the server for monitoring purposes.
 *
 * @param {String} serverUniqId
 * @param {Number} timeout  which is the number of seconds before a server is considered not responsive.
 */
function createOne(serverUniqId, timeout) {
  const serverStatus = new ServerStatus({
    id: serverUniqId,
    alive: new Date(),
    revision: Date.now(), // important on server restart
    start: new Date(),
    timeout
  });
  serverStatuses[serverUniqId] = serverStatus;
  zerv.notifyCreation('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});

  return serverStatus;
}

function updateOne(serverStatus) {
  serverStatus.revision ++;
  serverStatus.alive = new Date();
  zerv.notifyUpdate('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});
}

function findAll() {
  return _.values(serverStatuses);
}

function findByServerId(serverId) {
  return serverStatuses[serverId];
}

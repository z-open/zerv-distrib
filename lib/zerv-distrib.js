
'strict mode';

const _ = require('lodash');

let zervCore;

_.forIn(require.cache, function(required) {
    if (required.exports && required.exports.apiRouter) {
        zervCore = required.exports;
        return false;
    }
});
if (!zervCore) {
    zervCore = require('zerv-core');
}

let zervSync;
_.forIn(require.cache, function(required) {
    if (required.exports && required.exports.notifyCreation) {
        zervSync = required.exports;
        return false;
    }
});
if (!zervSync) {
    zervSync = require('zerv-sync');
}


let zervDistrib;
_.forIn(require.cache, function(required) {
    if (required.exports && required.exports.submitProcess) {
        zervDistrib = required.exports;
        return false;
    }
});

// prevent creating another instance of this module
if (zervDistrib) {
    module.exports = zervDistrib;
} else {
    const IoRedis = require('ioredis');

    const processMonitorClient = require('./process-monitor-client.service');
    const processMonitorServer = require('./process-monitor-server.service');
    const processService = require('./process.service');
    const serverStatusService = require('./server-status.service');
    const serverConsoleService = require('./server-console.service');

    let redisClient;

    const service = {
        setCustomProcessImpl,
        submitProcess: processMonitorClient.submitProcess,
        waitForCompletion: processMonitorClient.waitForCompletion,
        monitorQueue: processMonitorServer.monitorQueue,
        addProcessType: processMonitorServer.addProcessType,
        formatClusterStatus,
        getServerId: processMonitorServer.getServerId,
        getRedisClient
    };

    zervCore.addModule('Distrib', service);

    processService.setZervDependency(zervCore);
    processMonitorClient.setZervDependency(zervCore);
    processMonitorServer.setZervDependency(zervCore);

    module.exports = service;


    const stopLocalServer = zervCore.stopLocalServer;

    zervCore.stopLocalServer = async (delay, exitDelay = 0) => {
        let serverStatus = serverStatusService.findByServerId(service.getServerId());
        serverStatus.state = 'shutdown in progress';
        serverStatusService.updateOne(serverStatus, []);
        await stopLocalServer(delay);
        serverStatus = serverStatusService.findByServerId(service.getServerId());
        serverStatus.state = 'shutdown';
        serverStatusService.updateOne(serverStatus, []);
        if (exitDelay) {
            setTimeout(() => {
                console.info('shutdown local server');
                process.exit();
            }, exitDelay * 1000);
        }
    };

  /**
   * Shutdown and exit the server after all activities currently in progress completed on all zerv servers
   * @param {Number} delayBeforeShuttingdown
   * @param {Number} postDelay is the delay after stopping all activies before exiting
   */
    let shuttingDown;
    zervCore.shutdown = async function shutdown(delayBeforeShuttingdown = 5, exitDelay = 5) {
        if (shuttingDown) {
            return;
        }
        shuttingDown = setTimeout(() => {
            const serverStatuses = serverStatusService.findAll();
            _.forEach(serverStatuses, (serverStatus) => {
                if (serverStatus.id !== zervCore.getServerId()) {
                    zervSync.notifyCreation('cluster', 'SERVER_SHUTDOWN', {serverId: serverStatus.id}, {allServers: true});
                }
            });
        }, delayBeforeShuttingdown);
        zervCore.stopLocalServer(delayBeforeShuttingdown, false);
        let h;
        await new Promise((resolve) => {
      // check that all servers are down on timely basis
            h = setInterval(() => {
                const newServerStatuses = serverStatusService.findAll();
                if (_.every(newServerStatuses, (serverStatus) => serverStatus.state === 'shutdown' || !serverStatus.isAlive())) {
                    console.log(JSON.stringify(newServerStatuses, null, 2));
                    resolve();
                }
            }, 1000);
        });
        clearInterval(h);
        setTimeout(() => {
            console.info('shutdown of the entire infrastructure completed');
            process.exit();
        }, exitDelay * 1000);
    };

  // starting point needs to be revised
    function setCustomProcessImpl() {
        serverStatusService.listenToServerStatusData(zervCore);
    }

  /**
   * this returns a redis client.
   * This function should be in zerv-sync comming from the cluster source code which declares on already.
   *
   * @returns {IoRedis}
   */
    function getRedisClient() {
        if (!redisClient && process.env.REDIS_ENABLED === 'true') {
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
        return redisClient;
    }

    function formatClusterStatus(findTenantByIdFn) {
        if (_.isEmpty(getRedisClient())) {
            return 'Redis client is disabled';
        };
        return serverConsoleService.getHtmlReport(findTenantByIdFn);
    }
}



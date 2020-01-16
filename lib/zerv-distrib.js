
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
  const processMonitorClient = require('./process-monitor-client.service');
  const processMonitorServer = require('./process-monitor-server.service');

  const service = {
    setCustomProcessImpl,
    submitProcess: processMonitorClient.submitProcess,
    waitForCompletion: processMonitorClient.waitForCompletion,
    monitorQueue: processMonitorServer.monitorQueue,
    addProcessType: processMonitorServer.addProcessType,
    formatClusterStatus
  };

  zervCore.addModule('Distrib', service);


  module.exports = service;


  const processService = require('./process.service');
  const serverStatusService = require('./server-status.service');


  function setCustomProcessImpl(impl) {
    processService.setProcessService(impl, zervCore);
    processMonitorClient.setProcessService(processService, zervCore);
    processMonitorServer.setProcessService(processService, zervCore);
    serverStatusService.listenToServerStatusData(zervCore);
  }

  async function formatClusterStatus(findTenandById) {
    const colors = {};
    colors[processService.PROCESS_STATUSES.IN_PROGRESS] = 'green';
    colors[processService.PROCESS_STATUSES.COMPLETE] = 'black';
    colors[processService.PROCESS_STATUSES.ERROR] = 'red';

    const processes = _.orderBy(await processService.findAll(), p => !p.timestamp.lastModifiedDate ? - Date.now() : -(new Date(p.timestamp.lastModifiedDate).getTime()));
    const queueTb = (await Promise.all(_.map(processes, formatProcess))).join('');
    const statuses = _.orderBy(await serverStatusService.findAll(), s => -(new Date(s.alive)));
    const statusTb = _.map(statuses, await formatServerStatus).join('');


    return (`<!doctype html>
    <html>
    <body>
    <h1>CLUSTER STATUS</h1>
    <table border="1" cellpadding="7">${statusTb}</table>
    <h1>CLUSTER PROCESS QUEUE</h1>
    <table border="1" cellpadding="7">${queueTb}</table>
    </body>
    </html>`);

    async function formatProcess(p) {
      const color = _.get(colors, p.status, 'grey');
      const tenant = await findTenandById(p.tenantId);
      return `
        <tr><td rowspan=2>${p.start}</td><td rowspan=2>${p.type}<br/>${p.name}<br/><b>${tenant.display}</b></td><td><span style="color:${color};font-weight:bold">${p.status}</span></td><td>${p.duration} seconds</td><td>${p.serverId}</td></tr>
        <tr><td colspan=3>${p.progressDescription} ${p.error ? `<br/>${p.error.description || p.error.message}` : ''} </td></tr>
        `;
    }

    function formatServerStatus(s) {
      return `
        <tr><td>${s.id}</td><td>${s.alive}</td><td>${s.isAlive() ? 'Alive' :'Offline'}<br/>Active processes: ${s.activeProcesses.length}</td></tr>
        `;
    }
  }
}



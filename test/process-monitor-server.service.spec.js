const _ = require('lodash');
const zerv = require('zerv-core');
const service = require('../lib/process-monitor-server.service');
const processMonitorClientService = require('../lib/process-monitor-client.service');
const processService = require('../lib/process.service');
const ioRedisLock = require('ioredis-lock');
const RedisClientMock= require('./redis-client-mock');
const serverStatusService = require('../lib/server-status.service');


describe('ProcessMonitorServerService', () => {
    let spec;
    let locks, redisMock;
    beforeEach(() => {
        locks = {};
        spec = {};
        spec.serverId1 = 'server1';
        spec.tenantId = 'tenantId1';
        spec.type = 'doSomething';
        spec.type2 = 'doSomething2';
        spec.type3 = 'doSomething3ButFail';

        redisMock = new RedisClientMock();
        zerv.getRedisClient = () => redisMock;

        zerv.onChanges = _.noop;
        zerv.notifyCreation = _.noop;
        zerv.notifyUpdate = _.noop;
        processMonitorClientService.setZervDependency(zerv);
        processService.setZervDependency(zerv);
        service.setZervDependency(zerv);

        service.addProcessType(spec.type, (tenantId, processHandle, params) => ({
            description: 'done',
            data: null
        }));

        service.addProcessType(spec.type2, (tenantId, processHandle, params) => ({
            description: 'done',
            data: null
        }));

        service.addProcessType(spec.type3, (tenantId, processHandle, params) => {
            throw new Error('FAIL');
        });


        spyOn(ioRedisLock, 'createLock').and.callFake(() => {
            let lock, releaseLock;
            const p = new Promise((resolve) => {
                releaseLock = resolve;
            });
            return {
                acquire: async (lockName) => {
                    lock = lockName;
                    const previous = locks[lock];
                    if (previous) {
                        await previous;
                    }
                    locks[lock] = p;
                },
                release: () => {
                    delete locks[lock];
                    releaseLock();
                }
            };
        });

        spyOn(serverStatusService, 'getServerId').and.returnValue(spec.serverId1);
    });

    describe('_selectNextProcessesToRun function', () => {
        it('should pick the submitted process in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});

            expect(redisMock.cache['zerv-active-queue']).toBeDefined();
            const cacheData = redisMock.hvals('zerv-active-queue');
            expect(cacheData.length).toEqual(1);
            const r = JSON.parse(cacheData[0]);

            expect(r).toEqual({
                id: jasmine.any(String),
                createdDate: jasmine.any(String),
                lastModifiedDate: jasmine.any(String),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 0,
                start: null,
                end: null,
                status: 'pending',
                progressDescription: null,
                serverId: null,
                data: null,
                error: null
            });
            service._setCapacityProfile(2);


            const next = await service._selectNextProcessesToRun();
            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should pick the 2 submitted process in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();

            expect(next.length).toEqual(2);

            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
            expect(next[1]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething2',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should pick only 2 submitted process among the 4 in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();

            expect(next.length).toEqual(2);

            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
            expect(next[1]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething2',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });
    });

    describe('_executeProcess function', () => {
        it('should complete the process successfully', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();
            const process = await service._executeProcess(next[0]);

            expect(process).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 3,
                start: jasmine.any(Date),
                end: jasmine.any(Date),
                status: 'complete',
                progressDescription: 'done',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should fail to complete the process', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type3, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();
            const process = await service._executeProcess(next[0]);

            expect(process).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething3ButFail',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 3,
                start: jasmine.any(Date),
                end: jasmine.any(Date),
                status: 'error',
                progressDescription: 'Started by server server1',
                serverId: 'server1',
                data: null,
                error: {message: 'FAIL', description: undefined}
            }));
        });
    });

    xdescribe('_checkIfProcessIsNotStalled', () => {
        it('should not throw any error as process is valid', () => {

        });
        it('should throw an error when process gracePeriod is over', () => {

        });
        it('should throw an error when process is not handled by the server owner', () => {

        });

    });

    describe('_runNextProcesses function', () => {

        beforeEach(() => {
            service._setCapacityProfile(2);
            spyOn(service, '_scheduleToCheckForNewProcessResquests');
            spyOn(service,'_runNextProcesses').and.callThrough();
        });

        it('should tract the process execution in the server active process map to track the load', async () => {
            spyOn(service, '_executeProcess').and.callFake(() => {
                expect(service._getActiveProcesses().length).toBe(1);
                return 'completed way or another';
            });

            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});

            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
        });

        it('should indicate that the queue is being processed to reduce access to the queue', async () => {
            expect(service._waitForProcessingQueue).toBeNull();
            const promise = service._runNextProcesses();
            expect(service._waitForProcessingQueue).not.toBeNull();
            expect(service._waitForProcessingQueue).toBeInstanceOf(Promise);
            await promise;
            expect(service._waitForProcessingQueue).toBeNull();
        });

        it('should not retry to check for new processes since the trigger is a notification', async () => {
            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
            expect(service._scheduleToCheckForNewProcessResquests).not.toHaveBeenCalled();     
        });

        it('should schedule and check the queue as soon as a process completes', async () => {
            spyOn(service, '_executeProcess').and.returnValue('completed way or another');
            service._scheduleToCheckForNewProcessResquests.and.callThrough();
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
      
            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(1);
            expect(service._runNextProcesses).toHaveBeenCalledTimes(2);
        });

    });
});

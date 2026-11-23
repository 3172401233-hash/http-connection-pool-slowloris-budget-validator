'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  createBudgetValidator,
  computeFairShare,
  PHASES,
  ACTIONS,
  RISK,
} = require('../src/budget-validator');

function makeConnection(overrides = {}) {
  return {
    id: 'conn-1',
    ip: '203.0.113.10',
    tenantId: 'tenant-a',
    phase: PHASES.HEADERS,
    ageMs: 0,
    bytesReceived: 0,
    ...overrides,
  };
}

test('slow header trickle is closed as slowloris after grace period', () => {
  const validator = createBudgetValidator({ poolCapacity: 10 });
  const conn = makeConnection({ ageMs: 5_000, bytesReceived: 10 });
  validator.registerConnection(conn);

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.CLOSE);
  assert.ok(decision.reasons.some((r) => r.includes('slowloris-suspect')));
  assert.equal(decision.connectionState.headerBytesPerSecond, 2);
  assert.ok(decision.releaseConditions.some((c) => c.includes('close-immediately')));
});

test('header phase exceeding header timeout is closed', () => {
  const validator = createBudgetValidator({ headerTimeoutMs: 8_000, poolCapacity: 10 });
  const conn = makeConnection({ ageMs: 8_500, bytesReceived: 4_096 });

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.CLOSE);
  assert.ok(decision.reasons.includes('header-timeout-exceeded'));
});

test('healthy header progress within grace waits', () => {
  const validator = createBudgetValidator({ poolCapacity: 10 });
  const conn = makeConnection({ ageMs: 1_000, bytesReceived: 512 });

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.WAIT);
  assert.equal(decision.occupancyRisk, RISK.LOW);
  assert.ok(decision.releaseConditions.some((c) => c.includes('release-when')));
});

test('long streaming response is not misjudged as slowloris', () => {
  const validator = createBudgetValidator({ poolCapacity: 10 });
  const conn = makeConnection({
    phase: PHASES.STREAMING_RESPONSE,
    ageMs: 600_000,
    bytesReceived: 0,
  });

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.WAIT);
  assert.ok(decision.reasons.some((r) => r.includes('long-download-allowed')));
});

test('slow upload is not misjudged as slowloris', () => {
  const validator = createBudgetValidator({ poolCapacity: 10, uploadTimeoutMs: 300_000 });
  const conn = makeConnection({
    phase: PHASES.UPLOAD,
    ageMs: 60_000,
    bytesReceived: 1_024,
  });

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.WAIT);
  assert.ok(decision.reasons.some((r) => r.includes('slow-upload-allowed')));
});

test('upload exceeding its own timeout is closed', () => {
  const validator = createBudgetValidator({ poolCapacity: 10, uploadTimeoutMs: 30_000 });
  const conn = makeConnection({
    phase: PHASES.UPLOAD,
    ageMs: 31_000,
    bytesReceived: 1_024,
  });

  const decision = validator.evaluate(conn);

  assert.equal(decision.action, ACTIONS.CLOSE);
  assert.ok(decision.reasons.includes('upload-timeout-exceeded'));
});

test('proxy shared IP: ip, proxy and tenant quotas are tracked separately', () => {
  const validator = createBudgetValidator({
    poolCapacity: 100,
    maxConnectionsPerIp: 4,
    maxConnectionsPerProxy: 3,
    enforceFairShare: false,
  });
  const sharedIp = '198.51.100.7';
  const conns = [];
  for (let i = 0; i < 3; i += 1) {
    const conn = makeConnection({
      id: `proxied-${i}`,
      ip: sharedIp,
      proxyId: 'proxy-1',
      tenantId: `tenant-${i}`,
      ageMs: 100,
      bytesReceived: 200,
    });
    validator.registerConnection(conn);
    conns.push(conn);
  }

  const snapshot = validator.quotaSnapshot(conns[0]);
  assert.equal(snapshot.ip.used, 3);
  assert.equal(snapshot.proxy.used, 3);
  assert.equal(snapshot.tenant.used, 1);

  const fourth = makeConnection({
    id: 'proxied-3',
    ip: sharedIp,
    proxyId: 'proxy-1',
    tenantId: 'tenant-3',
    ageMs: 100,
    bytesReceived: 200,
  });
  validator.registerConnection(fourth);
  const decision = validator.evaluate(fourth);

  assert.equal(decision.action, ACTIONS.CLOSE);
  assert.ok(decision.reasons.includes('quota-exceeded:proxy'));
  assert.equal(decision.quota.proxy.used, 4);
  assert.equal(decision.quota.proxy.cap, 3);
});

test('same IP without proxy uses ip quota; different tenants are separate buckets', () => {
  const validator = createBudgetValidator({
    poolCapacity: 100,
    maxConnectionsPerIp: 2,
    enforceFairShare: false,
  });
  const first = makeConnection({ id: 'a', tenantId: 'tenant-a' });
  const second = makeConnection({ id: 'b', tenantId: 'tenant-b' });
  validator.registerConnection(first);
  validator.registerConnection(second);

  const snapshotA = validator.quotaSnapshot(first);
  const snapshotB = validator.quotaSnapshot(second);
  assert.equal(snapshotA.ip.used, 2);
  assert.equal(snapshotA.tenant.used, 1);
  assert.equal(snapshotB.tenant.used, 1);

  const third = makeConnection({ id: 'c', tenantId: 'tenant-c', ageMs: 100, bytesReceived: 100 });
  validator.registerConnection(third);
  const decision = validator.evaluate(third);
  assert.equal(decision.action, ACTIONS.CLOSE);
  assert.ok(decision.reasons.includes('quota-exceeded:ip'));
});

test('quota recovers after connections are released', () => {
  const validator = createBudgetValidator({
    poolCapacity: 10,
    maxConnectionsPerIp: 2,
    enforceFairShare: false,
  });
  const first = makeConnection({ id: 'a' });
  const second = makeConnection({ id: 'b' });
  validator.registerConnection(first);
  validator.registerConnection(second);

  const third = makeConnection({ id: 'c', ageMs: 100, bytesReceived: 100 });
  validator.registerConnection(third);
  assert.equal(validator.evaluate(third).action, ACTIONS.CLOSE);

  validator.releaseConnection(first);
  validator.releaseConnection(second);
  validator.releaseConnection(third);

  assert.equal(validator.quotaSnapshot(first).ip.used, 0);
  assert.equal(validator.poolState().inUse, 0);

  const fresh = makeConnection({ id: 'd', ageMs: 100, bytesReceived: 100 });
  validator.registerConnection(fresh);
  const decision = validator.evaluate(fresh);
  assert.equal(decision.action, ACTIONS.WAIT);
  assert.equal(decision.quota.ip.used, 1);
});

test('exhausted backend pool rejects new header-phase connections', () => {
  const validator = createBudgetValidator({ poolCapacity: 2, enforceFairShare: false });
  validator.registerConnection(makeConnection({ id: 'a', ip: '203.0.113.1' }));
  validator.registerConnection(makeConnection({ id: 'b', ip: '203.0.113.2' }));

  const conn = makeConnection({ id: 'c', ip: '203.0.113.3', ageMs: 100, bytesReceived: 100 });
  const decision = validator.evaluate(conn, validator.poolState());

  assert.equal(decision.action, ACTIONS.REJECT);
  assert.equal(decision.occupancyRisk, RISK.HIGH);
  assert.ok(decision.reasons.includes('pool-exhausted'));
});

test('fair share shrinks per-key cap as more keys become active', () => {
  assert.equal(computeFairShare(100, 1, 32), 32);
  assert.equal(computeFairShare(100, 4, 32), 25);
  assert.equal(computeFairShare(100, 200, 32), 1);
  assert.equal(computeFairShare(100, 0, 32), 32);
});

test('occupancy risk reflects pool watermarks', () => {
  const validator = createBudgetValidator({ poolCapacity: 10, poolHighWatermark: 0.8 });
  assert.equal(validator.occupancyRisk({ capacity: 10, inUse: 2 }), RISK.LOW);
  assert.equal(validator.occupancyRisk({ capacity: 10, inUse: 6 }), RISK.MEDIUM);
  assert.equal(validator.occupancyRisk({ capacity: 10, inUse: 9 }), RISK.HIGH);
});

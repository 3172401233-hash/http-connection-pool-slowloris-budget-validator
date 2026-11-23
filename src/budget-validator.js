'use strict';

const PHASES = Object.freeze({
  HEADERS: 'headers',
  UPLOAD: 'upload',
  STREAMING_RESPONSE: 'streaming-response',
  IDLE: 'idle',
});

const ACTIONS = Object.freeze({
  WAIT: 'wait',
  REJECT: 'reject',
  CLOSE: 'close',
});

const RISK = Object.freeze({
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
});

const DEFAULT_CONFIG = Object.freeze({
  headerTimeoutMs: 10_000,
  headerGraceMs: 2_000,
  minHeaderBytesPerSecond: 16,
  uploadTimeoutMs: 120_000,
  streamTimeoutMs: 0,
  maxConnectionsPerIp: 32,
  maxConnectionsPerProxy: 256,
  maxConnectionsPerTenant: 512,
  poolCapacity: 100,
  poolHighWatermark: 0.85,
  enforceFairShare: true,
});

function clampShare(value) {
  return Math.max(1, Math.floor(value));
}

function computeFairShare(poolCapacity, activeKeyCount, configuredCap) {
  if (activeKeyCount <= 0) {
    return clampShare(Math.min(configuredCap, poolCapacity));
  }
  const evenShare = poolCapacity / activeKeyCount;
  return clampShare(Math.min(configuredCap, evenShare));
}

class QuotaTracker {
  constructor() {
    this.buckets = new Map();
  }

  key(scope, id) {
    return `${scope}:${id}`;
  }

  count(scope, id) {
    return this.buckets.get(this.key(scope, id)) ?? 0;
  }

  acquire(scope, id) {
    const key = this.key(scope, id);
    const next = (this.buckets.get(key) ?? 0) + 1;
    this.buckets.set(key, next);
    return next;
  }

  release(scope, id) {
    const key = this.key(scope, id);
    const current = this.buckets.get(key) ?? 0;
    if (current <= 1) {
      this.buckets.delete(key);
      return 0;
    }
    this.buckets.set(key, current - 1);
    return current - 1;
  }

  activeKeys(scope) {
    let count = 0;
    for (const key of this.buckets.keys()) {
      if (key.startsWith(`${scope}:`)) {
        count += 1;
      }
    }
    return count;
  }
}

class BudgetValidator {
  constructor(config = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.quotas = new QuotaTracker();
    this.poolInUse = 0;
  }

  registerConnection(connection) {
    this.quotas.acquire('ip', connection.ip);
    if (connection.proxyId) {
      this.quotas.acquire('proxy', connection.proxyId);
    }
    this.quotas.acquire('tenant', connection.tenantId);
    this.poolInUse += 1;
  }

  releaseConnection(connection) {
    this.quotas.release('ip', connection.ip);
    if (connection.proxyId) {
      this.quotas.release('proxy', connection.proxyId);
    }
    this.quotas.release('tenant', connection.tenantId);
    this.poolInUse = Math.max(0, this.poolInUse - 1);
  }

  poolState() {
    return { capacity: this.config.poolCapacity, inUse: this.poolInUse };
  }

  occupancyRisk(pool = this.poolState()) {
    const ratio = pool.capacity > 0 ? pool.inUse / pool.capacity : 1;
    if (ratio >= this.config.poolHighWatermark) {
      return RISK.HIGH;
    }
    if (ratio >= this.config.poolHighWatermark * 0.7) {
      return RISK.MEDIUM;
    }
    return RISK.LOW;
  }

  quotaSnapshot(connection) {
    const cfg = this.config;
    const ipActive = this.quotas.activeKeys('ip');
    const tenantActive = this.quotas.activeKeys('tenant');
    const ipCap = cfg.enforceFairShare
      ? computeFairShare(cfg.poolCapacity, ipActive, cfg.maxConnectionsPerIp)
      : cfg.maxConnectionsPerIp;
    const tenantCap = cfg.enforceFairShare
      ? computeFairShare(cfg.poolCapacity, tenantActive, cfg.maxConnectionsPerTenant)
      : cfg.maxConnectionsPerTenant;
    return {
      ip: { used: this.quotas.count('ip', connection.ip), cap: ipCap },
      proxy: connection.proxyId
        ? { used: this.quotas.count('proxy', connection.proxyId), cap: cfg.maxConnectionsPerProxy }
        : null,
      tenant: { used: this.quotas.count('tenant', connection.tenantId), cap: tenantCap },
    };
  }

  quotaExceeded(snapshot) {
    if (snapshot.ip.used > snapshot.ip.cap) {
      return 'ip';
    }
    if (snapshot.proxy && snapshot.proxy.used > snapshot.proxy.cap) {
      return 'proxy';
    }
    if (snapshot.tenant.used > snapshot.tenant.cap) {
      return 'tenant';
    }
    return null;
  }

  evaluate(connection, pool = this.poolState()) {
    const cfg = this.config;
    const reasons = [];
    const releaseConditions = [];
    const risk = this.occupancyRisk(pool);
    const quota = this.quotaSnapshot(connection);
    const exceededScope = this.quotaExceeded(quota);
    const poolFull = pool.inUse >= pool.capacity;

    const state = {
      id: connection.id ?? null,
      phase: connection.phase,
      ageMs: connection.ageMs,
      bytesReceived: connection.bytesReceived,
      headerBytesPerSecond:
        connection.ageMs > 0
          ? connection.bytesReceived / (connection.ageMs / 1000)
          : connection.bytesReceived,
    };

    if (connection.phase === PHASES.HEADERS) {
      if (connection.ageMs >= cfg.headerTimeoutMs) {
        reasons.push('header-timeout-exceeded');
        releaseConditions.push('close-immediately: header phase exceeded headerTimeoutMs');
        return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
      }
      const pastGrace = connection.ageMs >= cfg.headerGraceMs;
      if (pastGrace && state.headerBytesPerSecond < cfg.minHeaderBytesPerSecond) {
        reasons.push('slowloris-suspect: header byte rate below minimum');
        releaseConditions.push('close-immediately: header trickle detected');
        return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
      }
      if (exceededScope) {
        reasons.push(`quota-exceeded:${exceededScope}`);
        releaseConditions.push(`close-immediately: ${exceededScope} quota exceeded during header phase`);
        return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
      }
      if (poolFull) {
        reasons.push('pool-exhausted');
        releaseConditions.push('reject: backend pool has no free slots');
        return this.#decision(ACTIONS.REJECT, state, risk, quota, reasons, releaseConditions);
      }
      reasons.push('header-phase-within-budget');
      releaseConditions.push(
        `release-when: headers complete, or ageMs >= ${cfg.headerTimeoutMs}, or byte rate stays below ${cfg.minHeaderBytesPerSecond} B/s after grace`,
      );
      return this.#decision(ACTIONS.WAIT, state, risk, quota, reasons, releaseConditions);
    }

    if (connection.phase === PHASES.UPLOAD) {
      if (cfg.uploadTimeoutMs > 0 && connection.ageMs >= cfg.uploadTimeoutMs) {
        reasons.push('upload-timeout-exceeded');
        releaseConditions.push('close-immediately: upload exceeded uploadTimeoutMs');
        return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
      }
      reasons.push('slow-upload-allowed: not a slowloris condition');
      releaseConditions.push(
        `release-when: upload completes, or ageMs >= ${cfg.uploadTimeoutMs || 'disabled'}`,
      );
      return this.#decision(ACTIONS.WAIT, state, risk, quota, reasons, releaseConditions);
    }

    if (connection.phase === PHASES.STREAMING_RESPONSE) {
      if (cfg.streamTimeoutMs > 0 && connection.ageMs >= cfg.streamTimeoutMs) {
        reasons.push('stream-timeout-exceeded');
        releaseConditions.push('close-immediately: stream exceeded streamTimeoutMs');
        return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
      }
      reasons.push('long-download-allowed: not a slowloris condition');
      releaseConditions.push(
        `release-when: response finishes, or ageMs >= ${cfg.streamTimeoutMs || 'disabled'}`,
      );
      return this.#decision(ACTIONS.WAIT, state, risk, quota, reasons, releaseConditions);
    }

    if (exceededScope) {
      reasons.push(`quota-exceeded:${exceededScope}`);
      releaseConditions.push(`close-immediately: ${exceededScope} quota exceeded`);
      return this.#decision(ACTIONS.CLOSE, state, risk, quota, reasons, releaseConditions);
    }
    reasons.push('idle-within-budget');
    releaseConditions.push('release-when: next request starts or idle connection is reaped');
    return this.#decision(ACTIONS.WAIT, state, risk, quota, reasons, releaseConditions);
  }

  #decision(action, state, risk, quota, reasons, releaseConditions) {
    return {
      action,
      connectionState: state,
      occupancyRisk: risk,
      quota,
      reasons,
      releaseConditions,
    };
  }
}

function createBudgetValidator(config) {
  return new BudgetValidator(config);
}

module.exports = {
  createBudgetValidator,
  BudgetValidator,
  computeFairShare,
  PHASES,
  ACTIONS,
  RISK,
};

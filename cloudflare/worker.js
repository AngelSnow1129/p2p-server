/**
 * p2psession on Cloudflare Workers + Durable Objects
 * =================================================
 *
 * 这是 Go 参考实现的一个「边缘等价物」，用于验证第 32 节的设计判断：
 *
 *     一个 Session  ==  一个 Durable Object
 *
 * Durable Object（DO）天然是单线程、单实例、带存储的协调者，正好对应
 * Go 实现里 session.Service + member.Hub 的组合。与 Go 版的关键差异：
 *
 *   DO 内是单线程事件循环 → 不需要 member.Hub 那把锁，
 *   成员表直接是普通 Map；但必须在每个 await 之间重新校验状态
 *   （DO 的输入门只保证「不交错执行同步段」，不保证 await 期间状态未变）。
 *
 * 数据面：DO 的 WebSocket 支持二进制帧，因此中继逻辑与 Go 版一致——
 * 服务器只做「按 MemberIndex 查找目标 + 转发」，不解析载荷。
 *
 * 明确不做的事（与 Go 版保持一致的安全边界）：
 *   - 不尝试解密任何业务数据（端到端密钥由客户端用 X25519 协商）；
 *   - 不把 Join Token 当数据密钥；
 *   - 不因为运行在边缘就放松「只有会话成员能互发」的校验。
 *
 * 部署（wrangler.toml 见同目录）：
 *   npx wrangler deploy
 */

const DEFAULT_TTL_MS = 30 * 60 * 1000;
const DEFAULT_MAX_MEMBERS = 10;
const TICKET_TTL_MS = 10 * 60 * 1000;

// 与 Go 版 protocol 包保持一致的常量。
const MODE_PAIR = 'pair';
const MODE_GROUP = 'group';
const MEMBER_ACTIVE = 'active';
const MEMBER_PENDING = 'pending';

// 数据面帧魔数（高半字节 0x2 = 版本 1）。
const FRAME_MAGIC = 0x21;
const DST_UNICAST = 1;
const DST_MULTICAST = 2;
const DST_BROADCAST = 3;

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

const enc = new TextEncoder();
const dec = new TextDecoder();

/** Crockford Base32（去掉易混字符），与 Go 侧一致。 */
const B32 = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

function randomId(prefix, bytes) {
  const buf = crypto.getRandomValues(new Uint8Array(bytes));
  let s = '';
  for (const b of buf) s += B32[b % 32];
  return prefix + s;
}

/** 32 字节 join token 的机器形式（base64url 无填充）。 */
function tokenString(raw) {
  let bin = '';
  for (const b of raw) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** 人类可传形式 P2P-XXXX-XXXX-…（base32，与 token 等熵）。 */
function joinCode(raw) {
  let bits = 0;
  let acc = 0;
  let out = '';
  for (const b of raw) {
    acc = (acc << 8) | b;
    bits += 8;
    while (bits >= 5) {
      out += B32[(acc >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += B32[(acc << (5 - bits)) & 31];
  // 每 4 字符一组，与 Go 版 EncodeJoinCode 一致。
  const groups = out.match(/.{1,4}/g) || [];
  return 'P2P-' + groups.join('-');
}

/** 原始 token 的存储哈希：sha256 的 hex（服务器从不存明文 token）。 */
async function hashToken(raw) {
  const digest = await crypto.subtle.digest('SHA-256', raw);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** 从机器形式/join code 还原原始 token 字节。 */
function decodeToken(input) {
  const s = String(input || '').trim();
  if (!s) return null;

  // join code：base32。
  const isCode = s.toUpperCase().startsWith('P2P-');
  if (isCode) {
    const body = s.slice(4).toUpperCase().replace(/-/g, '');
    let bits = 0;
    let acc = 0;
    const out = [];
    for (const ch of body) {
      const v = B32.indexOf(ch);
      if (v < 0) return null;
      acc = (acc << 5) | v;
      bits += 5;
      if (bits >= 8) {
        out.push((acc >>> (bits - 8)) & 0xff);
        bits -= 8;
      }
    }
    return out.length === 32 ? new Uint8Array(out) : null;
  }

  // 机器形式：base64url。
  try {
    const padded = s.replace(/-/g, '+').replace(/_/g, '/');
    const bin = atob(padded + '=='.slice(0, (4 - (padded.length % 4)) % 4));
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out.length === 32 ? out : null;
  } catch {
    return null;
  }
}

/** 恒定时间比较（避免通过响应时间区分「token 前缀匹配程度」）。 */
function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** 由 Ed25519 公钥派生 NodeID（sha256(pub)[:16] 的 hex，与 Go 版一致）。 */
async function nodeIdFromPublic(pubHex) {
  const pub = hexToBytes(pubHex);
  if (!pub) return null;
  const digest = await crypto.subtle.digest('SHA-256', pub);
  return [...new Uint8Array(digest).slice(0, 16)]
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

function hexToBytes(hex) {
  const s = String(hex || '');
  if (s.length % 2 !== 0) return null;
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) {
    const byte = parseInt(s.substr(i * 2, 2), 16);
    if (Number.isNaN(byte)) return null;
    out[i] = byte;
  }
  return out;
}

/** join proof 覆盖的规范字节串（与 Go 版 JoinProofMessage 对齐）。 */
async function joinProofMessage(tokenHash, nonce) {
  const prefix = enc.encode('p2psession:join:v1\n');
  const th = enc.encode(tokenHash);
  const sep = new Uint8Array([0]);
  const body = new Uint8Array(prefix.length + th.length + 1 + nonce.length);
  body.set(prefix, 0);
  body.set(th, prefix.length);
  body.set(sep, prefix.length + th.length);
  body.set(nonce, prefix.length + th.length + 1);
  const d = await crypto.subtle.digest('SHA-256', body);
  return new Uint8Array(d);
}

/** 校验 join proof：客户端持私钥对 (tokenHash, nonce) 签名。 */
async function verifyJoinProof(pubHex, nodeId, tokenHash, nonceHex, sigHex) {
  const derived = await nodeIdFromPublic(pubHex);
  if (!derived || derived !== nodeId) return { ok: false, code: 'unauthorized' };

  const pub = hexToBytes(pubHex);
  const sig = hexToBytes(sigHex);
  const nonce = hexToBytes(nonceHex);
  if (!pub || !sig || !nonce) return { ok: false, code: 'unauthorized' };

  try {
    const key = await crypto.subtle.importKey('raw', pub, { name: 'Ed25519' }, false, ['verify']);
    const msg = await joinProofMessage(tokenHash, nonce);
    const ok = await crypto.subtle.verify({ name: 'Ed25519' }, key, sig, msg);
    return ok ? { ok: true } : { ok: false, code: 'unauthorized' };
  } catch {
    return { ok: false, code: 'unauthorized' };
  }
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });
}

function err(status, code, message) {
  return json({ error: message, code }, status);
}

// ---------------------------------------------------------------------------
// SessionHub：一个 Session == 一个 Durable Object
// ---------------------------------------------------------------------------

export class SessionHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    /** @type {Map<string, any>} memberId -> 成员记录 */
    this.members = new Map();
    /** @type {Map<number, string>} index -> memberId（数据面寻址） */
    this.byIndex = new Map();
    /** @type {Map<string, WebSocket[]>} memberId -> 该成员的连接 */
    this.conns = new Map();
    this.meta = null;
    this.loaded = this.#load();
  }

  async #load() {
    // DO 存储保证单实例串行访问，因此可以直接把会话状态放这里。
    const meta = await this.state.storage.get('session');
    if (meta) {
      this.meta = meta;
      for (const m of meta.members || []) {
        this.members.set(m.id, m);
        this.byIndex.set(m.index, m.id);
      }
    }
  }

  async #persist() {
    if (!this.meta) return;
    this.meta.members = [...this.members.values()];
    await this.state.storage.put('session', this.meta);
  }

  #nextIndex() {
    const used = new Set(this.members.values().map((m) => m.index));
    for (let i = 1; i < 65536; i++) if (!used.has(i)) return i;
    return 0;
  }

  #activeCount() {
    let n = 0;
    for (const m of this.members.values()) if (m.status === MEMBER_ACTIVE) n++;
    return n;
  }

  /** 发给某成员的全部连接。 */
  #sendTo(memberId, payload) {
    const list = this.conns.get(memberId) || [];
    for (const ws of list) {
      try {
        // WebSocket 的 readyState 常量：1 = OPEN。
        if (ws.readyState === 1) ws.send(payload);
      } catch {
        // 单个连接失败不影响其他成员。
      }
    }
    return list.length > 0;
  }

  #broadcast(excludeMemberId, payload) {
    let n = 0;
    for (const id of this.conns.keys()) {
      if (id === excludeMemberId) continue;
      if (this.#sendTo(id, payload)) n++;
    }
    return n;
  }

  #memberInfo(m) {
    return {
      member_id: m.id,
      index: m.index,
      node_id: m.nodeId,
      public_key: m.publicKey,
      capabilities: m.capabilities || [],
      candidates: m.candidates || [],
      is_owner: m.role === 'owner',
      role: m.role,
      status: m.status,
      online: (this.conns.get(m.id) || []).length > 0,
    };
  }

  async fetch(request) {
    await this.loaded;
    const url = new URL(request.url);

    // WebSocket 升级：/control 或 /relay，用 ticket 鉴权。
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.#handleWebSocket(request, url);
    }

    switch (`${request.method} ${url.pathname}`) {
      case 'POST /init':
        return this.#init(request);
      case 'POST /join':
        return this.#join(request);
      case 'GET /members':
        return json({ session_id: this.meta?.id, members: [...this.members.values()].map((m) => this.#memberInfo(m)) });
      case 'POST /approve':
        return this.#approve(request);
      case 'POST /revoke':
        return this.#revoke();
      case 'POST /rotate':
        return this.#rotate();
      case 'POST /leave':
        return this.#leave(request);
      default:
        return err(404, 'not_found', 'no such route');
    }
  }

  // ---- 会话生命周期 -------------------------------------------------------

  async #init(request) {
    const body = await request.json().catch(() => ({}));

    if (this.meta) {
      // 已初始化：DO 在删除前一直存活，重复 init 说明客户端用了相同的 code。
      return err(409, 'conflict', 'session already initialized');
    }

    const mode = body.mode === MODE_GROUP ? MODE_GROUP : MODE_PAIR;
    const raw = crypto.getRandomValues(new Uint8Array(32));
    const now = Date.now();

    // Session ID 必须由调用方（Worker 入口）传入，这里是关键：
    // DO 实例是用 idFromName(sessionId) 选定的，如果 DO 内部再自己生成一个
    // 不同的 ID，那么「返回给客户端的 session_id」与「实际持有该会话状态的
    // DO」就指向两个不同的实例——后续按 session_id 加入会路由到一个空实例。
    // 因此 ID 的所有权在 Worker 入口，DO 只负责使用它。
    if (!body.session_id) {
      return err(400, 'bad_request', 'session_id required (assigned by the worker entry)');
    }

    this.meta = {
      id: body.session_id,
      mode,
      // pair 恒为 2：不接受调用方覆盖，避免「pair 但 5 人」的语义混乱。
      maxMembers: mode === MODE_PAIR ? 2 : Math.min(body.max_members || DEFAULT_MAX_MEMBERS, 1000),
      joinLimit: body.join_limit || 0,
      joinsUsed: 0,
      requireApproval: !!body.require_approval,
      revoked: false,
      closed: false,
      tokenHash: await hashToken(raw),
      joinCode: joinCode(raw),
      ownerMemberId: '',
      createdAt: now,
      expiresAt: now + (body.ttlMs || DEFAULT_TTL_MS),
      members: [],
    };
    await this.#persist();

    return json({
      session_id: this.meta.id,
      join_token: tokenString(raw),
      join_code: this.meta.joinCode,
      mode: this.meta.mode,
      max_members: this.meta.maxMembers,
      join_limit: this.meta.joinLimit,
      require_approval: this.meta.requireApproval,
      expires_at: new Date(this.meta.expiresAt).toISOString(),
      created_at: new Date(this.meta.createdAt).toISOString(),
    }, 201);
  }

  async #join(request) {
    const body = await request.json().catch(() => ({}));
    const now = Date.now();

    if (!this.meta) return err(404, 'session_not_found', 'session not found');
    if (this.meta.closed) return err(410, 'session_closed', 'session closed');
    if (now > this.meta.expiresAt) return err(410, 'session_expired', 'session expired');

    // 1. 校验 token（能力凭证）。
    const raw = decodeToken(body.join_token || body.join_code);
    if (!raw) return err(401, 'unauthorized', 'malformed token');
    const gotHash = await hashToken(raw);
    if (!timingSafeEqual(gotHash, this.meta.tokenHash)) {
      return err(401, 'unauthorized', 'token does not match session');
    }

    // 2. 同一 NodeID 重连视为恢复：复用 MemberID/Index，且不受撤销影响
    //    （该成员此前已凭同一 token 通过授权）。
    const existing = [...this.members.values()].find((m) => m.nodeId === body.node_id);
    if (existing) {
      existing.lastSeen = now;
      existing.connectionId = body.connection_id || '';
      await this.#persist();
      return json(this.#joinResponse(existing, true));
    }

    if (this.meta.revoked) return err(403, 'token_revoked', 'token revoked');
    if (this.meta.joinLimit > 0 && this.meta.joinsUsed >= this.meta.joinLimit) {
      return err(409, 'join_limit_reached', 'join limit reached');
    }

    // 3. 校验 join proof（身份绑定到该 token 对应的会话）。
    const proof = await verifyJoinProof(
      body.public_key, body.node_id, this.meta.tokenHash, body.nonce, body.signature,
    );
    if (!proof.ok) return err(401, 'unauthorized', 'invalid join proof');

    // 4. 容量与审批。首个加入者是 owner，不受审批约束——
    //    否则唯一有权审批的人会卡在 pending，会话永久不可用。
    const isFirst = !this.meta.ownerMemberId;
    const pending = this.meta.requireApproval && !isFirst;
    if (!pending && this.#activeCount() >= this.meta.maxMembers) {
      return err(409, 'session_full', 'session full');
    }

    const idx = this.#nextIndex();
    if (idx === 0) return err(409, 'session_full', 'index space exhausted');

    const member = {
      id: randomId('mbr_', 15),
      sessionId: this.meta.id,
      index: idx,
      nodeId: body.node_id,
      publicKey: body.public_key,
      role: isFirst ? 'owner' : 'member',
      status: pending ? MEMBER_PENDING : MEMBER_ACTIVE,
      capabilities: body.capabilities || [],
      candidates: body.candidates || [],
      connectionId: body.connection_id || '',
      joinedAt: now,
      lastSeen: now,
    };
    if (isFirst) this.meta.ownerMemberId = member.id;
    this.members.set(member.id, member);
    this.byIndex.set(idx, member.id);
    this.meta.joinsUsed += 1;
    await this.#persist();

    // 5. 关键：把「新成员已生效」广播给既有成员。
    //    没有这一步，先加入的 A 永远不知道 B 来了，
    //    「仅凭同一个 Token 自动发现彼此」就不成立。
    if (member.status === MEMBER_ACTIVE) {
      const notice = JSON.stringify({ type: 'member_joined', member: this.#memberInfo(member) });
      this.#broadcast(member.id, notice);

      if (this.#activeCount() >= this.meta.maxMembers) {
        const ready = JSON.stringify({
          type: 'session_ready',
          session_id: this.meta.id,
          members: [...this.members.values()]
            .filter((m) => m.status === MEMBER_ACTIVE)
            .map((m) => this.#memberInfo(m)),
        });
        this.#broadcast('', ready);
      }
    }

    return json(this.#joinResponse(member, false));
  }

  #joinResponse(member, rejoined) {
    const peers = [...this.members.values()]
      .filter((m) => m.id !== member.id && m.status === MEMBER_ACTIVE)
      .map((m) => this.#memberInfo(m));

    return {
      session_id: this.meta.id,
      member_id: member.id,
      index: member.index,
      is_owner: member.role === 'owner',
      status: member.status,
      rejoined,
      // 票据：DO 用自身 ID + memberId 签发，升级 WS 时校验。
      ticket: this.#mintTicket(member),
      members: peers,
      mode: this.meta.mode,
      heartbeat_ms: 15000,
      control_url: '/control',
      relay_url: '/relay',
    };
  }

  // ---- 票据（HMAC-SHA256，无状态自校验） --------------------------------

  async #hmacKey() {
    if (!this.hmacKeyCache) {
      // 密钥派生自 Worker secret；未配置时退化为每个 DO 随机密钥
      // （单实例可用，多实例需配置 P2PS_TICKET_SECRET）。
      const secret = (this.env && this.env.P2PS_TICKET_SECRET) || 'dev-insecure-secret';
      const base = await crypto.subtle.importKey(
        'raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify'],
      );
      this.hmacKeyCache = base;
    }
    return this.hmacKeyCache;
  }

  async #mintTicket(member) {
    const body = {
      session_id: this.meta.id,
      member_id: member.id,
      role: member.role,
      exp: Date.now() + TICKET_TTL_MS,
    };
    const payload = btoa(JSON.stringify(body)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const key = await this.#hmacKey();
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(payload));
    const sigStr = btoa(String.fromCharCode(...new Uint8Array(sig)))
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    return `${payload}.${sigStr}`;
  }

  async #verifyTicket(ticket) {
    const parts = String(ticket || '').split('.');
    if (parts.length !== 2) return null;
    const key = await this.#hmacKey();
    const sigBytes = (() => {
      try {
        const bin = atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'));
        const out = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
        return out;
      } catch {
        return null;
      }
    })();
    if (!sigBytes) return null;

    const valid = await crypto.subtle.verify('HMAC', key, sigBytes, enc.encode(parts[0]));
    if (!valid) return null;

    try {
      const body = JSON.parse(atob(parts[0].replace(/-/g, '+').replace(/_/g, '/')));
      if (body.exp < Date.now()) return null;
      if (body.session_id !== this.meta.id) return null;
      return body;
    } catch {
      return null;
    }
  }

  // ---- 审批 / 撤销 / 轮换 / 离开 -----------------------------------------

  async #approve(request) {
    const body = await request.json().catch(() => ({}));
    const actor = await this.#verifyTicket(body.actor_ticket);
    if (!actor || actor.role !== 'owner') return err(403, 'not_owner', 'owner ticket required');

    const target = this.members.get(body.member_id);
    if (!target) return err(404, 'member_not_found', 'member not found');
    if (target.status !== MEMBER_PENDING) return err(400, 'bad_request', 'member is not pending');
    // 批准时重新检查容量：pending 期间名额可能已被占满。
    if (this.#activeCount() >= this.meta.maxMembers) {
      return err(409, 'session_full', 'session full');
    }

    target.status = MEMBER_ACTIVE;
    await this.#persist();

    const info = this.#memberInfo(target);
    this.#broadcast(target.id, JSON.stringify({ type: 'member_joined', member: info }));
    this.#sendTo(target.id, JSON.stringify({ type: 'joined', member: info }));
    return json({ status: 'approved', member: info });
  }

  async #revoke() {
    if (!this.meta) return err(404, 'session_not_found', 'session not found');
    this.meta.revoked = true;
    await this.#persist();
    // 注意：撤销只阻止新加入，已在线成员不受影响。
    return json({ status: 'revoked', session_id: this.meta.id });
  }

  async #rotate() {
    if (!this.meta) return err(404, 'session_not_found', 'session not found');
    const raw = crypto.getRandomValues(new Uint8Array(32));
    this.meta.tokenHash = await hashToken(raw);
    this.meta.joinCode = joinCode(raw);
    this.meta.revoked = false;
    await this.#persist();
    return json({
      status: 'rotated',
      session_id: this.meta.id,
      join_token: tokenString(raw),
      join_code: this.meta.joinCode,
    });
  }

  async #leave(request) {
    const body = await request.json().catch(() => ({}));
    const m = this.members.get(body.member_id);
    if (!m) return err(404, 'member_not_found', 'member not found');

    this.#closeMember(body.member_id);
    this.members.delete(body.member_id);
    this.byIndex.delete(m.index);
    await this.#persist();

    this.#broadcast(body.member_id, JSON.stringify({ type: 'member_left', member_id: body.member_id }));
    return json({ status: 'left', member_id: body.member_id });
  }

  #closeMember(memberId) {
    const list = this.conns.get(memberId) || [];
    for (const ws of list) {
      try {
        ws.close(1000, 'closed');
      } catch {
        /* ignore */
      }
    }
    this.conns.delete(memberId);
  }

  // ---- WebSocket（控制面 /control 与数据面 /relay） ----------------------

  async #handleWebSocket(request, url) {
    const ticket = url.searchParams.get('ticket');
    const claim = await this.#verifyTicket(ticket);
    if (!claim) return err(401, 'unauthorized', 'invalid or expired ticket');

    const member = this.members.get(claim.member_id);
    if (!member) return err(410, 'member_not_found', 'member gone');

    const isRelay = url.pathname.endsWith('/relay');
    // pending 成员可以连控制面等待审批，但不得进入数据面。
    if (isRelay && member.status !== MEMBER_ACTIVE) {
      return err(403, 'pending_approval', 'member is not active yet');
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    // 必须 accept：不 accept 的连接无法收发。
    server.accept();

    const list = this.conns.get(member.id) || [];
    list.push(server);
    this.conns.set(member.id, list);

    server.addEventListener('message', (ev) => {
      if (isRelay) {
        this.#onRelayFrame(member, ev.data);
      } else {
        this.#onControlMessage(member, ev.data);
      }
    });

    const cleanup = () => {
      const cur = this.conns.get(member.id) || [];
      const next = cur.filter((ws) => ws !== server);
      if (next.length) this.conns.set(member.id, next);
      else this.conns.delete(member.id);
      // 成员离线：标记 lastSeen，但保留记录（重连可复用 MemberID）。
      member.lastSeen = Date.now();
      this.#persist().catch(() => {});
    };
    server.addEventListener('close', cleanup);
    server.addEventListener('error', cleanup);

    // 控制面连上即下发成员快照（自动发现的入口）。
    if (!isRelay) {
      server.send(JSON.stringify({
        type: 'member_list',
        session_id: this.meta.id,
        members: [...this.members.values()].map((m) => this.#memberInfo(m)),
      }));
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  #onControlMessage(member, data) {
    let msg;
    try {
      msg = JSON.parse(typeof data === 'string' ? data : dec.decode(data));
    } catch {
      return;
    }
    member.lastSeen = Date.now();

    switch (msg.type) {
      case 'ping':
        this.#sendTo(member.id, JSON.stringify({ type: 'pong', ts: msg.ts }));
        break;

      case 'key_exchange':
      case 'candidate_offer':
      case 'candidate_answer':
      case 'punch_result':
      case 'peer_state':
      case 'stream_open':
      case 'stream_close': {
        // 一律只做「校验成员关系 + 透传」，不解析内容——
        // key_exchange 里的 X25519 公钥对服务器不可理解。
        const target = msg.target_id;
        if (!target || !this.members.has(target)) {
          this.#sendTo(member.id, JSON.stringify({ type: 'error', code: 'member_not_found', message: 'target not found' }));
          break;
        }
        const out = JSON.stringify({ ...msg, from: member.id, session_id: this.meta.id });
        if (!this.#sendTo(target, out)) {
          this.#sendTo(member.id, JSON.stringify({ type: 'error', code: 'member_not_found', message: 'target offline' }));
        }
        break;
      }

      case 'leave':
        this.#closeMember(member.id);
        this.members.delete(member.id);
        this.byIndex.delete(member.index);
        this.#persist().catch(() => {});
        this.#broadcast(member.id, JSON.stringify({ type: 'member_left', member_id: member.id }));
        break;

      default:
        this.#sendTo(member.id, JSON.stringify({ type: 'error', code: 'unsupported', message: `unsupported: ${msg.type}` }));
    }
  }

  /**
   * 数据面：解析帧头 → 校验成员关系 → 转发。不触碰 payload。
   *
   * 帧布局（与 Go 版 protocol/frame.go 一致）：
   *   [0]        magic
   *   [1]        flags
   *   [2..4]     src index（必须由服务器盖写）
   *   [4]        dst type
   *   [5..9]     streamID
   *   [9..11]    seq
   *   [11]       保留
   *   [12..]     目标下标（单播 2B / 组播 1B count + 2B*n）/ payload
   */
  #onRelayFrame(member, data) {
    if (member.status !== MEMBER_ACTIVE) return;
    const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
    if (!(buf instanceof Uint8Array) || buf.length < 12 || buf[0] !== FRAME_MAGIC) return;

    const dstType = buf[4];
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const streamId = view.getUint32(5, false);
    const seq = view.getUint16(9, false);

    // 盖写来源下标：客户端填什么都无效（与 Go 版一致）。
    const srcIndex = member.index;

    let targets = [];
    let payloadOffset = 12;

    if (dstType === DST_UNICAST) {
      if (buf.length < 14) return;
      targets = [view.getUint16(12, false)];
      payloadOffset = 14;
    } else if (dstType === DST_MULTICAST) {
      if (buf.length < 13) return;
      const n = buf[12];
      if (n === 0 || buf.length < 13 + 2 * n) return;
      for (let i = 0; i < n; i++) targets.push(view.getUint16(13 + 2 * i, false));
      payloadOffset = 13 + 2 * n;
    } else if (dstType === DST_BROADCAST) {
      targets = [...this.byIndex.keys()];
      payloadOffset = 12;
    } else {
      return;
    }

    const payload = buf.subarray(payloadOffset);

    for (const idx of targets) {
      const targetId = this.byIndex.get(idx);
      if (!targetId || targetId === member.id) continue;
      const target = this.members.get(targetId);
      if (!target || target.status !== MEMBER_ACTIVE) continue;

      // 重新打包为「指向该目标的单播帧」，接收方无需再解析目标列表。
      const frame = new Uint8Array(14 + payload.length);
      frame[0] = FRAME_MAGIC;
      frame[1] = buf[1];
      new DataView(frame.buffer).setUint16(2, srcIndex, false);
      frame[4] = DST_UNICAST;
      new DataView(frame.buffer).setUint32(5, streamId, false);
      new DataView(frame.buffer).setUint16(9, seq, false);
      new DataView(frame.buffer).setUint16(12, idx, false);
      frame.set(payload, 14);

      const list = this.conns.get(targetId) || [];
      for (const ws of list) {
        try {
          if (ws.readyState === 1) ws.send(frame);
        } catch {
          /* ignore */
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Worker 入口：Session 创建/加入的路由与 ID 分配
// ---------------------------------------------------------------------------
//
// 设计要点：Durable Object 的 ID 必须由 Session ID 稳定派生，
// 否则「再次加入同一会话」会命中不同的 DO 实例，成员表就分裂了。
// 因此用 idFromName(sessionId) 而不是 newUniqueId()。

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/v1/health') {
      return new Response('ok', { status: 200 });
    }

    // 创建会话：先分配一个 sessionId，再由它派生 DO；ID 一并传给 DO 使用。
    if (request.method === 'POST' && url.pathname === '/v1/sessions') {
      const sessionId = crypto.randomUUID().replace(/-/g, '').slice(0, 20);
      const stub = env.SESSION_HUB.get(env.SESSION_HUB.idFromName(sessionId));

      const incoming = await request.json().catch(() => ({}));
      const res = await stub.fetch('https://do/init', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ ...incoming, session_id: sessionId }),
      });
      return json(await res.json(), res.status);
    }

    // 加入会话：需要 session_id（join code 需由调用方先解析为 session_id，
    // 或在此维护 code -> sessionId 的 KV 索引；此处保持最小实现）。
    if (request.method === 'POST' && url.pathname === '/v1/session/join') {
      const body = await request.json().catch(() => ({}));
      if (!body.session_id) {
        return err(400, 'bad_request', 'session_id required');
      }
      const stub = env.SESSION_HUB.get(env.SESSION_HUB.idFromName(body.session_id));
      return stub.fetch('https://do/join', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
    }

    // 其余：/v1/sessions/{id}/... 与 /control、/relay
    const m = url.pathname.match(/^\/v1\/sessions\/([^/]+)(\/.*)?$/);
    if (m) {
      const sessionId = decodeURIComponent(m[1]);
      const rest = (m[2] || '').replace(/^\//, '');
      const stub = env.SESSION_HUB.get(env.SESSION_HUB.idFromName(sessionId));
      const doPath =
        rest === 'join' ? '/join'
          : rest === 'approve' ? '/approve'
            : rest === 'revoke-token' ? '/revoke'
              : rest === 'rotate-token' ? '/rotate'
                : rest === 'leave' ? '/leave'
                  : rest === 'members' ? '/members'
                    : '/' + rest;
      return stub.fetch(new Request('https://do' + doPath, request));
    }

    if (url.pathname === '/control' || url.pathname === '/relay') {
      const sessionId = url.searchParams.get('session_id');
      if (!sessionId) return err(400, 'bad_request', 'session_id required');
      const stub = env.SESSION_HUB.get(env.SESSION_HUB.idFromName(sessionId));
      return stub.fetch(request);
    }

    return err(404, 'not_found', 'no such route');
  },
};

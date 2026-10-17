import Foundation

/// Persist the outcome independently of the HTTP reply/watchdog. A client
/// queries the same ID after a timeout; duplicate IDs never dispatch twice.
final class OperationLedger {
    enum Admission {
        case accepted
        case existing([String: JSONValue])
        case rejected(JSONRPCError)
    }
    private struct Receipt: Codable {
        var client: String
        var id: String
        var method: String
        var params: [String: JSONValue]
        var state: String
        var updated: Double
        var result: [String: JSONValue]?
        var error: [String: JSONValue]?
    }
    private let lock = NSLock()
    private let url: URL
    private var receipts: [String: Receipt] = [:]
    private var storageError = false
    let session = UUID().uuidString

    init(url: URL) {
        self.url = url
        do {
            if FileManager.default.fileExists(atPath: url.path) {
                receipts = try JSONDecoder().decode([String: Receipt].self, from: Data(contentsOf: url))
                for key in Array(receipts.keys) where ["queued", "running"].contains(receipts[key]!.state) {
                    receipts[key]!.state = "unknown_after_restart"
                }
                try save()
            }
        } catch { storageError = true }
    }

    private func save() throws {
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try JSONEncoder().encode(receipts).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }

    private func value(_ receipt: Receipt) -> [String: JSONValue] {
        var out: [String: JSONValue] = ["operation_id": .string(receipt.id), "method": .string(receipt.method),
            "state": .string(receipt.state), "server_session": .string(session)]
        if let result = receipt.result { out["result"] = .object(result) }
        if let error = receipt.error { out["error"] = .object(error) }
        return out
    }

    func status(client: String, id: String) -> [String: JSONValue] {
        lock.lock(); defer { lock.unlock() }
        if storageError { return ["operation_id": .string(id), "state": .string("storage_unavailable")] }
        guard let receipt = receipts[client + ":" + id] else {
            return ["operation_id": .string(id), "state": .string("not_found"), "server_session": .string(session)]
        }
        return value(receipt)
    }

    func begin(client: String, id: String, method: String, params: [String: JSONValue]) -> Admission {
        lock.lock(); defer { lock.unlock() }
        guard !storageError, !client.isEmpty, id.range(of: "^[A-Za-z0-9_-]{16,80}$", options: .regularExpression) != nil else {
            return .rejected(.invalidParams)
        }
        let key = client + ":" + id
        if let old = receipts[key] {
            guard old.method == method && old.params == params else { return .rejected(.invalidParams) }
            return .existing(value(old))
        }
        // Never silently evict an ID and accidentally allow its replay.
        guard receipts.count < 50_000 else { return .rejected(.serverError("Operation receipt store is full; no action dispatched.")) }
        receipts[key] = Receipt(client: client, id: id, method: method, params: params,
            state: "queued", updated: Date().timeIntervalSince1970)
        do { try save(); return .accepted }
        catch { receipts.removeValue(forKey: key); storageError = true; return .rejected(.internalError) }
    }

    func running(client: String, id: String) -> Bool {
        lock.lock(); defer { lock.unlock() }
        let key = client + ":" + id
        guard !storageError, receipts[key]?.state == "queued" else { return false }
        receipts[key]!.state = "running"
        do { try save(); return true } catch { storageError = true; return false }
    }

    func finish(client: String, id: String, result: [String: JSONValue]?, error: JSONRPCError?) {
        lock.lock(); defer { lock.unlock() }
        let key = client + ":" + id
        guard receipts[key] != nil else { return }
        receipts[key]!.state = error == nil ? "succeeded" : "failed"
        receipts[key]!.result = result
        receipts[key]!.error = error.map { ["code": .int($0.code), "message": .string($0.message)] }
        receipts[key]!.updated = Date().timeIntervalSince1970
        // Retain ID tombstones permanently; only old response payloads expire.
        let full = receipts.filter { $0.value.result != nil || $0.value.error != nil }.sorted { $0.value.updated > $1.value.updated }
        for (oldKey, _) in full.dropFirst(512) {
            receipts[oldKey]!.result = nil; receipts[oldKey]!.error = nil
            receipts[oldKey]!.state = "completed_result_expired"
        }
        do { try save() } catch { storageError = true }
    }
}

// MARK: - JSON-RPC 2.0 Types

struct JSONRPCRequest: Codable {
    let jsonrpc: String
    let method: String
    let params: [String: JSONValue]?
    let id: JSONValue?
}

struct JSONRPCResponse: Encodable {
    let jsonrpc: String = "2.0"
    let result: [String: JSONValue]?
    let error: JSONRPCError?
    let id: JSONValue?
}

struct JSONRPCError: Encodable {
    let code: Int
    let message: String
    let data: [String: JSONValue]?

    static let parseError = JSONRPCError(code: -32700, message: "Parse error", data: nil)
    static let invalidRequest = JSONRPCError(code: -32600, message: "Invalid Request", data: nil)
    static let methodNotFound = JSONRPCError(code: -32601, message: "Method not found", data: nil)
    static let invalidParams = JSONRPCError(code: -32602, message: "Invalid params", data: nil)
    static let internalError = JSONRPCError(code: -32603, message: "Internal error", data: nil)
    /// Auth failure. Distinct code (-32099) so the HTTP layer can map ONLY this to
    /// 401 — business errors use serverError(-32000) and must return HTTP 200 with
    /// the failure in the JSON-RPC error body (otherwise a slow/timed-out action
    /// looks like "unauthorized" to clients).
    static let unauthorizedCode = -32099
    static let unauthorized = JSONRPCError(code: unauthorizedCode, message: "Unauthorized: valid auth token required", data: nil)

    static func serverError(_ msg: String) -> JSONRPCError {
        JSONRPCError(code: -32000, message: msg, data: nil)
    }
}

// MARK: - JSON Value (heterogeneous)

enum JSONValue: Codable, Equatable {
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)
    case null
    case array([JSONValue])
    case object([String: JSONValue])

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let v = try? container.decode(Bool.self) {
            self = .bool(v)
        } else if let v = try? container.decode(Int.self) {
            self = .int(v)
        } else if let v = try? container.decode(Double.self) {
            self = .double(v)
        } else if let v = try? container.decode(String.self) {
            self = .string(v)
        } else if let v = try? container.decode([JSONValue].self) {
            self = .array(v)
        } else if let v = try? container.decode([String: JSONValue].self) {
            self = .object(v)
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let v): try container.encode(v)
        case .int(let v): try container.encode(v)
        case .double(let v): try container.encode(v)
        case .bool(let v): try container.encode(v)
        case .null: try container.encodeNil()
        case .array(let v): try container.encode(v)
        case .object(let v): try container.encode(v)
        }
    }

    var stringValue: String? {
        if case .string(let v) = self { return v }
        return nil
    }

    var doubleValue: Double? {
        switch self {
        case .double(let v): return v
        case .int(let v): return Double(v)
        default: return nil
        }
    }

    var boolValue: Bool? {
        if case .bool(let v) = self { return v }
        return nil
    }

    var intValue: Int? {
        switch self {
        case .int(let v): return v
        case .double(let v): return Int(v)      // JSON numbers often parse as double
        default: return nil
        }
    }

    var objectValue: [String: JSONValue]? {
        if case .object(let v) = self { return v }
        return nil
    }

    func toAny() -> Any {
        switch self {
        case .string(let v): return v
        case .int(let v): return v
        case .double(let v): return v
        case .bool(let v): return v
        case .null: return NSNull()
        case .array(let v): return v.map { $0.toAny() }
        case .object(let v): return v.mapValues { $0.toAny() }
        }
    }
}

// MARK: - Auth Types

struct AuthRequest: Codable {
    let clientName: String

    enum CodingKeys: String, CodingKey {
        case clientName = "client_name"
    }
}

struct AuthResponse: Encodable {
    let token: String
    let expiresIn: Int?

    enum CodingKeys: String, CodingKey {
        case token
        case expiresIn = "expires_in"
    }
}

/// One paired client. `id` is the token id (first 4 bytes of SHA256(K), hex) —
/// it is what the wire sends as `tok` and is a lookup handle only, never a
/// secret. `keyHex` is the actual 32-byte key K used to derive the per-request
/// MAC/encryption keys; it never leaves the device after pairing.
///
/// Expiry is idle-based: a session dies only after `inactivityWindow` of no use.
/// Every authenticated request slides `lastActiveAt` forward, so a token in
/// regular use never expires.
struct AuthSession: Identifiable, Codable {
    let id: String
    let keyHex: String
    let clientName: String
    let grantedAt: Date
    var lastActiveAt: Date
    /// Where the last authenticated request came from: the peer address
    /// (no port) and how it arrived — see `PeerSource`. Nil for sessions not
    /// used since this was recorded.
    var lastAddress: String?
    var lastVia: String?

    /// The 32-byte key, or nil if the stored record is corrupt.
    var key: Data? { Data(hexString: keyHex) }

    /// "Expiry if never used again" — informational, recomputed from last use.
    var expiresAt: Date { lastActiveAt.addingTimeInterval(AuthManager.inactivityWindow) }

    var isExpired: Bool { Date() >= expiresAt }

    enum CodingKeys: String, CodingKey {
        case id
        case keyHex = "key"
        case clientName = "client_name"
        case grantedAt = "granted_at"
        case lastActiveAt = "last_active_at"
        case lastAddress = "last_address"
        case lastVia = "last_via"
    }
}

/// How a request reached the server, judged by the interface it arrived on.
enum PeerSource: String {
    /// Loopback: an app on this device, or a computer over USB (usbmuxd
    /// forwards to the device's loopback, so the two look the same).
    case local
    /// Wi-Fi, Ethernet or the Personal Hotspot bridge.
    case lan
    /// Apple peer-to-peer Wi-Fi (AWDL): nearby discovery.
    case nearby
    /// A VPN or proxy tunnel (utun, ipsec, ppp).
    case vpn
    case cellular
    case other
}

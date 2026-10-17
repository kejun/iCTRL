import Foundation

@main struct LedgerChecks {
    static func main() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("receipts.json")
        let id = "operation_1234567890"
        let ledger = OperationLedger(url: url)
        guard case .accepted = ledger.begin(client: "a", id: id, method: "page", params: ["direction": .string("up")]) else { fatalError() }
        assert(ledger.running(client: "a", id: id))
        assert(ledger.status(client: "b", id: id)["state"] == .string("not_found"))
        guard case .existing(let running) = ledger.begin(client: "a", id: id, method: "page", params: ["direction": .string("up")]) else { fatalError() }
        assert(running["state"] == .string("running"))
        guard case .rejected = ledger.begin(client: "a", id: id, method: "tap", params: [:]) else { fatalError() }
        ledger.finish(client: "a", id: id, result: ["moved": .bool(true)], error: nil)
        let reopened = OperationLedger(url: url)
        assert(reopened.status(client: "a", id: id)["state"] == .string("succeeded"))
        assert(reopened.status(client: "a", id: id)["result"] == .object(["moved": .bool(true)]))
        guard case .accepted = reopened.begin(client: "a", id: "pending_1234567890", method: "tap", params: [:]) else { fatalError() }
        let restarted = OperationLedger(url: url)
        assert(restarted.status(client: "a", id: "pending_1234567890")["state"] == .string("unknown_after_restart"))
        guard case .existing = restarted.begin(client: "a", id: "pending_1234567890", method: "tap", params: [:]) else { fatalError() }
        print("PASS: persistent receipts, duplicate suppression, client isolation, parameter binding, restart ambiguity")
    }
}

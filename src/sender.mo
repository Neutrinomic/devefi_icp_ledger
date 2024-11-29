import BTree "mo:stableheapbtreemap/BTree";
import Option "mo:base/Option";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Float "mo:base/Float";
import Ledger "./icp_ledger";
import Principal "mo:base/Principal";
import Vector "mo:vector";
import Timer "mo:base/Timer";
import Array "mo:base/Array";
import Error "mo:base/Error";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat32 "mo:base/Nat32";
import Time "mo:base/Time";
import Int "mo:base/Int";
import Prim "mo:⛔";
import Nat8 "mo:base/Nat8";
import TxTypes "./txtypes";
import IT "mo:itertools/Iter";
import Iter "mo:base/Iter";
import Set "mo:map/Set";
import Ver1 "./memory/v1";
import MU "mo:mosup";

module {

    public module Mem {
        public module Sender {
            public let V1 = Ver1.Sender;
        };
    };

    let VM = Mem.Sender.V1;

    public type TransactionInput = {
        amount: Nat;
        to: Ledger.Account;
        from_subaccount : ?Blob;
    };


    let RETRY_EVERY_SEC:Float = 120_000_000_000;
    let MAX_SENT_EACH_CYCLE:Nat = 125;

    let permittedDriftNanos : Nat64 = 60_000_000_000;
    let transactionWindowNanos : Nat64 = 86400_000_000_000;
    let retryWindow : Nat64 = 172800_000_000_000; // 2 x transactionWindowNanos

    let maxReaderLag : Nat64 = 1800_000_000_000; // 30 minutes


    private func adjustTXWINDOW(now:Nat64, time : Nat64) : Nat64 {
        // If tx is still not sent after the transaction window, we need to
        // set its created_at_time to the current window or it will never be sent no matter how much we retry.
        if (time >= now - retryWindow) return time;
        let window_idx = now / retryWindow;
        return window_idx * retryWindow;
    };



    public class Sender<system>({
        xmem : MU.MemShell<VM.Mem>;
        ledger_id: Principal;
        onError: (Text) -> ();
        onConfirmations : ([Nat64]) -> ();
        getFee : () -> Nat;
        onCycleEnd : (Nat64) -> (); // Measure performance of following and processing transactions. Returns instruction count
        isRegisteredAccount : (Blob) -> Bool;
        me_can: Principal;
    }) {
        let mem = MU.access(xmem);

        let ledger = actor(Principal.toText(ledger_id)) : Ledger.Oneway;
        var getReaderLastUpdateTime : ?(() -> (Nat64)) = null;

        let blobMin = "\00" : Blob;
        let blobMax = "\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF\FF" : Blob;


        public func setGetReaderLastUpdateTime(fn : () -> (Nat64)) {
            getReaderLastUpdateTime := ?fn;
        };


        private func cycle() : async () {

            let inst_start = Prim.performanceCounter(1); // 1 is preserving with async

            let fee = getFee();

            let now = Int.abs(Time.now());
            let nowU64 = Nat64.fromNat(now);

            let transactions_to_send = BTree.scanLimit<Blob, VM.Transaction>(mem.transactions, Blob.compare, blobMin, blobMax, #fwd, 3000);

            let ?gr_fn = getReaderLastUpdateTime else Debug.trap("Err getReaderLastUpdateTime not set");
            let lastReaderTxTime = gr_fn();  // This is the last time the reader has seen a transaction or the current time if there are no more transactions

            if (lastReaderTxTime != 0 and lastReaderTxTime < nowU64 - maxReaderLag) {
                onError("Reader is lagging behind by " # Nat64.toText(nowU64 - lastReaderTxTime));
                return; // Don't attempt to send transactions if the reader is lagging too far behind
            };

            var sent_count = 0;
            label vtransactions for ((id, tx) in transactions_to_send.results.vals()) {
                
                if (tx.amount < fee) {
                    ignore BTree.delete<Blob, VM.Transaction>(mem.transactions, Blob.compare, id);
                    ignore do ? { Set.delete(mem.transaction_ids, Set.n64hash, txBlobToId(id)!); };
                    continue vtransactions;
                };

                let time_for_try = Float.toInt(Float.ceil((Float.fromInt(now - Nat64.toNat(tx.created_at_time)))/RETRY_EVERY_SEC));

                if (tx.tries >= time_for_try) continue vtransactions;
                
                let created_at_adjusted = adjustTXWINDOW(nowU64, tx.created_at_time);

                try {
                    
                    // Relies on transaction deduplication https://github.com/dfinity/ICRC-1/blob/main/standards/ICRC-1/README.md
                    ledger.icrc1_transfer({
                        amount = tx.amount - fee;
                        to = tx.to;
                        from_subaccount = tx.from_subaccount;
                        created_at_time = ?created_at_adjusted;
                        memo = ?tx.memo;
                        fee = null;
                    });
                    sent_count += 1;
                    tx.tries := Int.abs(time_for_try);
                } catch (e) { 
                    onError("sender:" # Error.message(e));
                    break vtransactions;
                };

                if (sent_count >= MAX_SENT_EACH_CYCLE) break vtransactions;
    
            };
    
            let inst_end = Prim.performanceCounter(1);
            onCycleEnd(inst_end - inst_start);
        };


        public func confirm(txs: [TxTypes.Transaction]) {

            let confirmations = Vector.new<Nat64>();
            label tloop for (tx in txs.vals()) {
                let imp = switch(tx) { // Our canister realistically will never be the ICP minter
                    case (#u_transfer(t)) {
                        {from=t.from; memo=t.memo};
                    };
                    case (#u_burn(b)) {
                        {from=b.from; memo=b.memo};
                    };
                    case (_) continue tloop;
                };
                // let ?tr = tx.transfer else continue tloop;
                // if (tr.from.owner != owner) continue tloop;

                // Check if from is registered subaccount owned by this canister
                if (not isRegisteredAccount(imp.from)) continue tloop; // Transaction not coming from us

                let ?memo = imp.memo else continue tloop;
                let ?id = DNat64(Blob.toArray(memo)) else continue tloop;
                
                ignore BTree.delete<Blob, VM.Transaction>(mem.transactions, Blob.compare, txIdBlob(imp.from, id));
                Set.delete(mem.transaction_ids, Set.n64hash, id);
                Vector.add<Nat64>(confirmations, id);
            };
            onConfirmations(Vector.toArray(confirmations));
        };

        private func txIdBlob(address:Blob, id:Nat64) : Blob { // address is 32 bytes always
            return Blob.fromArray(Iter.toArray(IT.flattenArray<Nat8>([Blob.toArray(address), ENat64(id)])));
        };

        private func txBlobToId(blob:Blob) : ?Nat64 {
            DNat64(Array.subArray(Blob.toArray(blob), 32, 8));
        };

        public func getPendingCount() : Nat {
            return BTree.size(mem.transactions);
        };

        public func send(id:Nat64, tx: TransactionInput) {
            let txr : VM.Transaction = {
                amount = tx.amount;
                to = tx.to;
                from_subaccount = tx.from_subaccount;
                var created_at_time = Nat64.fromNat(Int.abs(Time.now()));
                memo = Blob.fromArray(ENat64(id));
                var tries = 0;
            };
            
            let account = Principal.toLedgerAccount(me_can, tx.from_subaccount);
            ignore BTree.insert<Blob, VM.Transaction>(mem.transactions, Blob.compare, txIdBlob(account, id), txr);
            ignore Set.put(mem.transaction_ids, Set.n64hash, id);
        };



        public func isSent(id:Nat64) : Bool {
            not Set.has(mem.transaction_ids, Set.n64hash, id);
        };


        public func ENat64(value : Nat64) : [Nat8] {
            return [
                Nat8.fromNat(Nat64.toNat(value >> 56)),
                Nat8.fromNat(Nat64.toNat((value >> 48) & 255)),
                Nat8.fromNat(Nat64.toNat((value >> 40) & 255)),
                Nat8.fromNat(Nat64.toNat((value >> 32) & 255)),
                Nat8.fromNat(Nat64.toNat((value >> 24) & 255)),
                Nat8.fromNat(Nat64.toNat((value >> 16) & 255)),
                Nat8.fromNat(Nat64.toNat((value >> 8) & 255)),
                Nat8.fromNat(Nat64.toNat(value & 255)),
            ];
        };

        public func DNat64(array : [Nat8]) : ?Nat64 {
            if (array.size() != 8) return null;
            return ?(Nat64.fromNat(Nat8.toNat(array[0])) << 56 | Nat64.fromNat(Nat8.toNat(array[1])) << 48 | Nat64.fromNat(Nat8.toNat(array[2])) << 40 | Nat64.fromNat(Nat8.toNat(array[3])) << 32 | Nat64.fromNat(Nat8.toNat(array[4])) << 24 | Nat64.fromNat(Nat8.toNat(array[5])) << 16 | Nat64.fromNat(Nat8.toNat(array[6])) << 8 | Nat64.fromNat(Nat8.toNat(array[7])));
        };

        ignore Timer.recurringTimer<system>(#seconds 2, cycle);

    };

};

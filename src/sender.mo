import BTree "mo:stableheapbtreemap/BTree";
import Option "mo:base/Option";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Float "mo:base/Float";
import Ledger "./icp_ledger";
import ICRCLedger "./icrc_ledger";
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
import Ver2 "./memory/v2";
import MU "mo:mosup";

module {

    public module Mem {
        public module Sender {
            public let V1 = Ver1.Sender;
            public let V2 = Ver2.Sender;
        };
    };

    let VM = Mem.Sender.V2;
    let VGM = Ver2;

    public type TransactionInput = {
        amount: Nat;
        to: VGM.AccountMixed;
        from_subaccount : ?Blob;
        memo : ?Blob;
    };


    let RETRY_EVERY_SEC:Float = 120_000_000_000;
    let MAX_SENT_EACH_CYCLE:Nat = 125;

    // let permittedDriftNanos : Nat64 = 60_000_000_000;
    // let transactionWindowNanos : Nat64 = 86400_000_000_000;
  
    public let retryWindow : Nat64 = 72200_000_000_000; // 2 x transactionWindowNanos

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
        onConfirmations : ([(Nat64, Nat)]) -> ();
        getFee : () -> Nat;
        onCycleEnd : (Nat64) -> (); // Measure performance of following and processing transactions. Returns instruction count
        isRegisteredAccount : (Blob) -> Bool;
        me_can: Principal;
        genNextSendId : (?Nat64) -> Nat64;
        
    }) {
        let mem = MU.access(xmem);
        let minter = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
        let minter_icrc = {owner=minter; subaccount=null};
        let minter_aid = Principal.toLedgerAccount(minter, null);

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
            label vtransactions for ((internal_id, tx) in transactions_to_send.results.vals()) {
                
                if (tx.amount <= fee) {
                    ignore BTree.delete<Blob, VM.Transaction>(mem.transactions, Blob.compare, internal_id);
                    ignore do ? { Set.delete(mem.transaction_ids, Set.n64hash, txBlobToId(internal_id)!); };
                    continue vtransactions;
                };

                let tx_aid = Principal.toLedgerAccount(me_can, tx.from_subaccount);

                let time_for_try = Float.toInt(Float.ceil((Float.fromInt(now - Nat64.toNat(tx.created_at_time)))/RETRY_EVERY_SEC));

                if (tx.tries >= time_for_try) continue vtransactions;
                
                var created_at_adjusted = adjustTXWINDOW(nowU64, tx.created_at_time);

                if (created_at_adjusted != tx.created_at_time) {
                    // Since we are now sending it with a different created_at_time, we need to delete the old one and insert the new one
                    // Or we won't be able to see it in the ledger
                    let old_id = internal_id;
                    let new_tsid = genNextSendId(?created_at_adjusted);
                    created_at_adjusted := new_tsid;
                    tx.created_at_time := created_at_adjusted;
                   
                    let new_id = txIdBlob(tx_aid, created_at_adjusted);
                    if (not BTree.has(mem.transactions, Blob.compare, new_id)) {
                        ignore BTree.insert<Blob, VM.Transaction>(mem.transactions, Blob.compare, new_id, tx);
                        ignore Set.put(mem.transaction_ids, Set.n64hash, created_at_adjusted);
                        ignore BTree.delete<Blob, VM.Transaction>(mem.transactions, Blob.compare, old_id);
                        ignore do ? { Set.delete(mem.transaction_ids, Set.n64hash, txBlobToId(old_id)!); };
                    } else {
                        continue vtransactions;
                    }
                };

                try {
                    
                    // Relies on transaction deduplication https://github.com/dfinity/ICRC-1/blob/main/standards/ICRC-1/README.md
                    switch(tx.to) {
                        case (#icrc(to)) {
                            if(to == minter_icrc) {
                                // BURN
                                    ledger.icrc1_transfer({
                                        amount = tx.amount;
                                        to = to;
                                        from_subaccount = tx.from_subaccount;
                                        created_at_time = ?created_at_adjusted;
                                        memo = ?tx.memo;
                                        fee = null;
                                    });
                                } else {
                                    ledger.icrc1_transfer({
                                        amount = tx.amount - fee;
                                        to = to;
                                        from_subaccount = tx.from_subaccount;
                                        created_at_time = ?created_at_adjusted;
                                        memo = ?tx.memo;
                                        fee = null;
                                    });
                                };
                           
                        };
                        case (#icp(to)) {
                            let nat64_memo:Nat64 = Option.get(DNat64(Blob.toArray(tx.memo)), 1:Nat64); 
         
                            if (to == minter_aid) {
                                // BURN
                                    ledger.transfer({
                                        amount = {e8s = Nat64.fromNat(tx.amount)};
                                        to = to;
                                        from_subaccount = tx.from_subaccount;
                                        created_at_time = ?{timestamp_nanos = created_at_adjusted};
                                        memo = nat64_memo;
                                        fee = {e8s = 0};
                                    });  
                                } else {
                                    ledger.transfer({
                                        amount = {e8s = Nat64.fromNat(tx.amount - fee)};
                                        to = to;
                                        from_subaccount = tx.from_subaccount;
                                        created_at_time = ?{timestamp_nanos = created_at_adjusted};
                                        memo = nat64_memo;
                                        fee = {e8s = Nat64.fromNat(fee)};
                                    });
                                }
                          
                            
                        };
                    };
                    
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


        public func confirm(txs: [TxTypes.Transaction], start_id: Nat) {

            let confirmations = Vector.new<(Nat64, Nat)>();
            label tloop for (idx in txs.keys()) {
                let tx=txs[idx];


                // After the upgrade we should just return created_at_time and simplify this code

                let imp = switch(tx) { // Our canister realistically will never be the ICP minter. Dont use the middleware for anything other than the real ICP
                    case (#u_transfer(t)) {
                        switch(t.memo) {
                            case (null) { // no icrc memo, we've used the icp memo
                                {from=t.from; id=t.created_at_time};
                            };
                            case (?memo) {
                               switch(memoToId(memo)) { 
                                case (null) {
                                    {from=t.from; id=t.created_at_time};
                                };
                                case (?id) {
                                    {from=t.from; id=id};
                                };
                            };
                            }
                        }
                    };
                    case (#u_burn(t)) {
                       switch(t.memo) {
                            case (null) { // no icrc memo, we've used the icp memo
                                {from=t.from; id=t.created_at_time};
                            };
                            case (?memo) {
                               switch(memoToId(memo)) { 
                                case (null) {
                                    {from=t.from; id=t.created_at_time};
                                };
                                case (?id) {
                                    {from=t.from; id=id};
                                };
                            };
                            }
                        }
                    };
                    case (_) continue tloop;
                };
   

                // Check if from is registered subaccount owned by this canister
                if (not isRegisteredAccount(imp.from)) continue tloop; // Transaction not coming from us
                // Registered subaccounts can only belong to this canister
                // And only this canister can send from them, so if it's registered, it comes from this canister
                
                let id = imp.id;

                ignore BTree.delete<Blob, VM.Transaction>(mem.transactions, Blob.compare, txIdBlob(imp.from, id));
                Set.delete(mem.transaction_ids, Set.n64hash, id);
                Vector.add<(Nat64, Nat)>(confirmations, (id, start_id + idx));


            };
            onConfirmations(Vector.toArray(confirmations));
        };

        private func txIdBlob(address:Blob, id:Nat64) : Blob { // address is 32 bytes always
            return Blob.fromArray(Iter.toArray(IT.flattenArray<Nat8>([Blob.toArray(address), ENat64(id)])));
        };

        private func txBlobToId(blob:Blob) : ?Nat64 {
            DNat64(Array.subArray(Blob.toArray(blob), 32, 8))
        };

        private func memoToId(memo:Blob) : ?Nat64 {
            let ?id = DNat64(Blob.toArray(memo)) else return null;
            if (id == 1 or id == 0) return null;
            return ?id;
        };

        public func getPendingCount() : Nat {
            return BTree.size(mem.transactions);
        };

        public func send(id:Nat64, tx: TransactionInput) {
            let txr : VM.Transaction = {
                amount = tx.amount;
                to = tx.to;
                from_subaccount = tx.from_subaccount;
                var created_at_time = id;
                memo = switch(tx.memo) {
                    case (null) Blob.fromArray(ENat64(1)); // Always needs memo for deduplication to work
                    case (?m) m;
                }; 
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

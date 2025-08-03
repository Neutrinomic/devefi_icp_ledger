import IcpReader "./reader";
import IcpSender "./sender";
import Principal "mo:base/Principal";
import Timer "mo:base/Timer";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Map "mo:map/Map";
import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Option "mo:base/Option";
import ICPLedger "./icp_ledger";
import ICRCLedger "./icrc_ledger";
import TxTypes "./txtypes";
import Debug "mo:base/Debug";
import SWB "mo:swb";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import BTree "mo:stableheapbtreemap/BTree";
import Ver1 "./memory/v1";
import Ver2 "./memory/v2";
import MU "mo:mosup";
import Time "mo:base/Time";
import Int "mo:base/Int";

module {
    type R<A,B> = Result.Result<A,B>;

    public module Mem {
        public module Ledger {
            public let V1 = Ver1.Ledger;
            public let V2 = Ver2.Ledger;
        };
    };

    let VM = Mem.Ledger.V2;

    public type Meta = VM.Meta;

    /// No other errors are currently possible
    public type SendError = {
        #InsufficientFunds;
        #InvalidSubaccount;
        #InvalidMemo;
        #UnsupportedAccount;
    };

 
    public func subaccountToBlob(s: ?Blob) : Blob {
        let ?a = s else return Blob.fromArray([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]);
        a;
    };

    public type TransactionShared = IcpSender.TransactionShared;

    /// Info about local ledger params returned by getInfo
    public type Info = {
        last_indexed_tx: Nat;
        accounts: Nat;
        pending: Nat;
        actor_principal: Principal;
        sender_instructions_cost : Nat64;
        reader_instructions_cost : Nat64;
        errors : Nat;
        lastTxTime: Nat64;
    };

    public type AccountMixed = {
        #icrc:ICRCLedger.Account;
        #icp:Blob;
    };

    public type Transfer = {
        to : ICRCLedger.Account;
        fee : ?Nat;
        from : AccountMixed;
        memo : ?Blob;
        created_at_time : ?Nat64;
        amount : Nat;
        spender : ?AccountMixed;
    };

    public type Settings = {
        CYCLE_RECURRING_TIME_SEC : Nat;
        START_FROM_BLOCK : { #id : Nat; #last };
        ME_CAN : Principal;
        LEDGER_ID: Principal;
    };
    
    /// The ledger class
    /// start_from_block should be in most cases #last (starts from the last block when first started)
    /// if something went wrong and you need to reinstall the canister
    /// or use the library with a canister that already has tokens inside it's subaccount balances
    /// you can set start_from_block to a specific block number from which you want to start reading the ledger when reinstalled
    /// you will have to remove all onRecieve, onSent, onMint, onBurn callbacks and set them again
    /// (or they could try to make calls based on old transactions)
    /// 
    /// Example:
    /// ```motoko
    ///     stable let lmem = L.LMem();
    ///     let ledger = L.Ledger(lmem, "bnz7o-iuaaa-aaaaa-qaaaa-cai", #last);
    /// ```
    public class Ledger<system>(xmem: MU.MemShell<VM.Mem>, settings: Settings) { //ledger_id_txt: Text, start_from_block : ({#id:Nat; #last}), me_can : Principal
        let lmem = MU.access(xmem);
        let ledger_id = settings.LEDGER_ID;
        let minter = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
        let me_can = settings.ME_CAN;

        let errors = SWB.SlidingWindowBuffer<Text>();

        var sender_instructions_cost : Nat64 = 0;
        var reader_instructions_cost : Nat64 = 0;

        var callback_onReceive: ?(<system>(Transfer) -> ()) = null;
        var callback_onSent : ?((Nat64, Nat) -> ()) = null;
        // Sender 


        private func logErr(e:Text) : () {
            let idx = errors.add(e);
            if ((1+idx) % 300 == 0) { // every 300 elements
                errors.delete( errors.len() - 100 ) // delete all but the last 100
            };
        };


        /// Called back with the id of the confirmed transaction. The id returned from the send function. Only one function can be set.
        public func onSent(fn : (Nat64, Nat) -> ()) : () {
            assert (Option.isNull(callback_onSent));
            callback_onSent := ?fn;
        };

        public func isRegisteredAccount(aid: Blob) : Bool {
            not Option.isNull(BTree.get(lmem.known_accounts, Blob.compare, aid));
        };


        public func genNextSendId(t: ?Nat64) : Nat64 {
            let created_at_time = (Option.get(t, Nat64.fromNat(Int.abs(Time.now())))/1_000_000_000)*1_000_000_000;
            

            let id = created_at_time + lmem.next_tx_id;
            lmem.next_tx_id += 1;
            if (lmem.next_tx_id >= 1_000_000_000) lmem.next_tx_id := 0;
            return id;
        };

        let icrc_sender = IcpSender.Sender<system>({
            CYCLE_RECURRING_TIME_SEC = settings.CYCLE_RECURRING_TIME_SEC;
            ledger_id;
            xmem = lmem.sender;
            getFee = func () : Nat { lmem.fee };
            onError = logErr; // In case a cycle throws an error
            onConfirmations = func (confirmations: [(Nat64, Nat)]) {
                // handle confirmed ids after sender 
                for (idx in confirmations.keys()) {
                    let (id, block_id) = confirmations[idx];
                    ignore do ? { callback_onSent!(id, block_id) };
                };
            };
            onCycleEnd = func (i: Nat64) { sender_instructions_cost := i }; // used to measure how much instructions it takes to send transactions in one cycle
            isRegisteredAccount;
            me_can;
            genNextSendId;
        });

        public let sender = icrc_sender;

        public func getPendingTransactions() : [IcpSender.TransactionShared] {
            icrc_sender.getPendingTransactions();
        };

        private func handle_incoming_amount(subaccount: ?Blob, amount: Nat) : () {
            switch(Map.get<Blob, VM.AccountMem>(lmem.accounts, Map.bhash, subaccountToBlob(subaccount))) {
                case (?acc) {
                    acc.balance += amount:Nat;
                };
                case (null) {
                    Map.set(lmem.accounts, Map.bhash, subaccountToBlob(subaccount), {
                        var balance = amount;
                        var in_transit = 0;
                    });
                };
            };
        };

        private func handle_outgoing_amount(subaccount: ?Blob, amount: Nat) : () {
            let ?acc = Map.get(lmem.accounts, Map.bhash, subaccountToBlob(subaccount)) else return;


            if (acc.balance < amount) {
                Debug.print("balance < amount");
                Debug.print(debug_show({balance = acc.balance; in_transit = acc.in_transit; amount = amount}));
                acc.balance := 0;
            } else {
                acc.balance -= amount:Nat;
            };


            // When replaying the ledger we don't have in_transit and it results in natural substraction underflow.
            // since in_transit is local and added when sending
            // we have to ignore if it when replaying
            // Also if for some reason devs decide to send funds with something else than this library, it will also be an amount that is not in transit
            if (acc.in_transit < amount) {
                acc.in_transit := 0;
            } else {
                acc.in_transit -= amount:Nat; 
            };

            if (acc.balance == 0 and acc.in_transit == 0) {
                ignore Map.remove<Blob, VM.AccountMem>(lmem.accounts, Map.bhash, subaccountToBlob(subaccount));
            };

        };

        let nullSubaccount:Blob = subaccountToBlob(null);
        // Usually we don't return 32 zeroes but null
        private func formatSubaccount(s: Blob) : ?Blob {
            if (s == nullSubaccount) null else ?s;
        };


        // Reader
        let icp_reader = IcpReader.Reader<system>({
            CYCLE_RECURRING_TIME_SEC = settings.CYCLE_RECURRING_TIME_SEC;
            xmem = lmem.reader;
            ledger_id;
            start_from_block = settings.START_FROM_BLOCK;
            onError = logErr; // In case a cycle throws an error
            onCycleEnd = func (i: Nat64) { reader_instructions_cost := i }; // returns the instructions the cycle used. 
                                                        // It can include multiple calls to onRead
            onRead = func <system>(transactions: [TxTypes.Transaction], start_id: Nat) {
                icrc_sender.confirm(transactions, start_id);
                
                
                label txloop for (tx in transactions.vals()) {
                    
                    switch(tx) {
                        case (#u_mint(mint)) {
                            let ?subaccount = BTree.get(lmem.known_accounts, Blob.compare, mint.to) else continue txloop;
                            handle_incoming_amount(?subaccount, mint.amount);
                            ignore do ? { callback_onReceive!<system>({
                                from = #icrc({
                                    owner = minter;
                                    subaccount = null;
                                });
                                to = {
                                    owner = me_can;
                                    subaccount = formatSubaccount(subaccount);
                                };
                                amount = mint.amount;
                                created_at_time = ?mint.created_at_time;
                                fee = null;
                                memo = mint.memo;
                                spender = null;
                                }); };
                        };

                        case (#u_transfer(tr)) {
                            let fee = tr.fee;
                            switch(BTree.get(lmem.known_accounts, Blob.compare, tr.to)) {
                                case (?subaccount) {

                                    if (tr.amount >= fee) { // ignore it since we can't even burn that
                                    handle_incoming_amount(?subaccount, tr.amount);

                                    let from_subaccount = BTree.get(lmem.known_accounts, Blob.compare, tr.from);
                                    ignore do ? { callback_onReceive!<system>({
                                        from = switch(from_subaccount) {
                                            case (?sa) #icrc({owner = me_can; subaccount = formatSubaccount(sa)});
                                            case (null) #icp(tr.from);
                                        };
                                        amount = tr.amount;
                                        to = {
                                            owner = me_can;
                                            subaccount = formatSubaccount(subaccount);
                                        };
                                        created_at_time = ?tr.created_at_time;
                                        fee = ?fee;
                                        memo = tr.memo;
                                        spender = do ? {#icp( tr.spender! )};
                                        });
                                        };
                                    }
                                };
                                case (null) ();
                            };
                      
                            switch(BTree.get(lmem.known_accounts, Blob.compare, tr.from)) {
                                case (?subaccount) {
                                    handle_outgoing_amount(formatSubaccount(subaccount), tr.amount + fee);
                                };
                                case (null) ();
                            };
                        };

                        case (#u_burn(burn)) {
                            let ?subaccount = BTree.get(lmem.known_accounts, Blob.compare, burn.from) else continue txloop;
                            handle_outgoing_amount(formatSubaccount(subaccount), burn.amount);
                        };

                        case (_) continue txloop;
                    };
                 
                };
            };
        });

        icrc_sender.setGetReaderLastUpdateTime(icp_reader.getReaderLastUpdate);

        icp_reader.optQueueSender := ?(icrc_sender.optQueueSend);

        private func refreshFee() : async () {
            try {
            let ledger = actor (Principal.toText(ledger_id)) : ICPLedger.Self;
            lmem.fee := await ledger.icrc1_fee();
            } catch (_e) {}
        };
 

        public func isSent(id : Nat64) : Bool {
            icrc_sender.isSent(id);
        };

        /// The ICP ledger doesn't know all of its subaccount addresses
        /// This why we need to register them, so it can track balances and transactions
        /// Any transactions to or from a subaccount before registering it will be ignored
        public func registerSubaccount(subaccount: ?Blob) : () {
            ignore BTree.insert<Blob, Blob>(lmem.known_accounts, Blob.compare, Principal.toLedgerAccount(me_can, subaccount), subaccountToBlob(subaccount));
        };

        public func unregisterSubaccount(subaccount: ?Blob) : () {
            ignore BTree.delete<Blob, Blob>(lmem.known_accounts, Blob.compare, Principal.toLedgerAccount(me_can, subaccount));
        };

        public func isRegisteredSubaccount(subaccount: ?Blob) : Bool {
            not Option.isNull(BTree.get(lmem.known_accounts, Blob.compare, Principal.toLedgerAccount(me_can, subaccount)));
        };
  

        public func getRegisteredAccount(aid: Blob) : ?ICRCLedger.Account {
            let ?subaccount = BTree.get(lmem.known_accounts, Blob.compare, aid) else return null;
            ?{owner=me_can; subaccount=formatSubaccount(subaccount)};
        };

        /// Returns the actor principal
        public func me() : Principal {
            me_can;
        };

        /// Returns the errors that happened
        public func getErrors() : [Text] {
            let start = errors.start();
            Array.tabulate<Text>(errors.len(), func (i:Nat) {
                let ?x = errors.getOpt(start + i) else Debug.trap("memory corruption");
                x
            });
        };
    
        /// Returns info about ledger library
        public func getInfo() : Info {
            {
                last_indexed_tx = icp_reader.getLastReadTxIndex();
                accounts = Map.size(lmem.accounts);
                pending = icrc_sender.getPendingCount();
                actor_principal = me_can;
                reader_instructions_cost;
                sender_instructions_cost;
                errors = errors.len();
                lastTxTime = icp_reader.getReaderLastTxTime();
            }
        };

        /// Get Iter of all accounts owned by the canister (except dust < fee)
        public func accounts() : Iter.Iter<(Blob, Nat)> {
            Iter.map<(Blob, VM.AccountMem), (Blob, Nat)>(Map.entries<Blob, VM.AccountMem>(lmem.accounts), func((k, v)) {
                (k, v.balance - v.in_transit)
            });
        };



        /// Returns the meta of the ICP ledger
        public func getMeta() : VM.Meta {
            { // These won't ever change for ICP except fee
                decimals = 8; 
                symbol = "ICP";
                fee = lmem.fee;
                minter = ?{ owner=minter; subaccount = null};
                name = "Internet Computer";
                max_memo = 32;
            }
        };

        /// Returns the fee for sending a transaction
        public func getFee() : Nat {
            lmem.fee;
        };

        /// Returns the ledger sender class
        public func getSender() : IcpSender.Sender {
            icrc_sender;
        };

        /// Returns the ledger reader class
        public func getReader() : IcpReader.Reader {
            icp_reader;
        };

        /// Send a transfer from a canister owned address
        /// It's added to a queue and will be sent as soon as possible.
        /// You can send tens of thousands of transactions in one update call. It just adds them to a BTree
        public func send(tr: IcpSender.TransactionInput) : R<Nat64, SendError> { // The amount we send includes the fee. meaning recepient will get the amount - fee
            let ?acc = Map.get(lmem.accounts, Map.bhash, subaccountToBlob(tr.from_subaccount)) else return #err(#InsufficientFunds);
            if (acc.balance:Nat - acc.in_transit:Nat < tr.amount) return #err(#InsufficientFunds);
            
            // Verify send to is valid
            switch(tr.to) {
                case (#icrc(to)) {
                    switch(to.subaccount) {
                        case (?subaccount) {
                            if (subaccount.size() != 32) return #err(#InvalidSubaccount);
                            ignore do ? { if (tr.memo!.size() > 32) return #err(#InvalidMemo)};
                        };
                        case (null) ();
                    };
                };
                case (#icp(to)) {
                    if (to.size() != 32) return #err(#InvalidSubaccount);
                    ignore do ? { if (tr.memo!.size() != 8) return #err(#InvalidMemo)};
                };
            };

            acc.in_transit += tr.amount;

            let id = genNextSendId(null);

            icrc_sender.send(id, tr);
            #ok(id);
        };

        /// Returns the balance of a subaccount owned by the canister (except dust < fee)
        /// It's different from the balance in the original ledger if sent transactions are not confirmed yet.
        /// We are keeping track of the in_transit amount.
        public func balance(subaccount:?Blob) : Nat {
            let ?acc = Map.get(lmem.accounts, Map.bhash, subaccountToBlob(subaccount)) else return 0;
            acc.balance - acc.in_transit;
        };

        /// Returns the internal balance in case we want to see in_transit and raw balance separately
        public func balanceInternal(subaccount:?Blob) : (Nat, Nat) {
            let ?acc = Map.get(lmem.accounts, Map.bhash, subaccountToBlob(subaccount)) else return (0,0);
            (acc.balance, acc.in_transit)
        };

        /// Called when a received transaction is confirmed. Only one function can be set. (except dust < fee)
        public func onReceive(fn:<system>(Transfer) -> ()) : () {
            assert(Option.isNull(callback_onReceive));
            callback_onReceive := ?fn;
        };

        registerSubaccount(null);

        /// Start the ledger timers
        ignore Timer.recurringTimer<system>(#seconds 3600, refreshFee);
        ignore Timer.setTimer<system>(#seconds 1, func() : async () {
           await refreshFee();
        });
    };


}
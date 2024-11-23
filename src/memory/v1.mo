import MU "mo:mosup";
import Map "mo:map/Map";
import BTree "mo:stableheapbtreemap/BTree";
import Set "mo:map/Set";

module {

    public module Ledger {
        public func new() : MU.MemShell<Mem> = MU.new<Mem>({
            reader = Reader.new();
            sender = Sender.new();
            accounts = Map.new<Blob, AccountMem>();
            known_accounts = BTree.init<Blob, Blob>(?16);
            var fee = 10000;
            var next_tx_id = 0;
        });

        /// Local account memory
        public type AccountMem = {
            var balance: Nat;
            var in_transit: Nat;
        };

        public type Mem = {
            reader: MU.MemShell<Reader.Mem>;
            sender: MU.MemShell<Sender.Mem>;
            accounts: Map.Map<Blob, AccountMem>;
            known_accounts : BTree.BTree<Blob, Blob>; // account id to subaccount
            var fee : Nat;
            var next_tx_id : Nat64;
        };

        public type Meta = {
            symbol: Text;
            name: Text;
            decimals: Nat8;
            fee: Nat;
            minter: ?Account;
        };
    };

    public module Reader {
        public func new() : MU.MemShell<Mem> = MU.new<Mem>({
            var last_indexed_tx = 0;
        });

        public type Mem = {
            var last_indexed_tx : Nat;
        };

    };

    public module Sender {
        public func new() : MU.MemShell<Mem> = MU.new<Mem>({
            transactions = BTree.init<Blob, Transaction>(?16);
            transaction_ids = Set.new<Nat64>();
        });

        public type Mem = {
            transactions : BTree.BTree<Blob, Transaction>;
            transaction_ids : Set.Set<Nat64>;
        };


        public type Transaction = {
            amount: Nat;
            to : Account;
            from_subaccount : ?Blob;
            var created_at_time : Nat64; // 1000000000
            memo : Blob;
            var tries: Nat;
        };
    };

    public type Account = { owner : Principal; subaccount : ?Blob };


}
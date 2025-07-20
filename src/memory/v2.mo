import MU "mo:mosup";
import Map "mo:map/Map";
import BTree "mo:stableheapbtreemap/BTree";
import Set "mo:map/Set";
import V1 "./v1";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";

module {

    public module Ledger {
        public func new() : MU.MemShell<Mem> = MU.new<Mem>({
            reader = Reader.new();
            sender = Sender.new();
            accounts = Map.new<Blob, AccountMem>();
            known_accounts = BTree.init<Blob, Blob>(?16);
            var fee = 10000;
            var next_tx_id = 100;
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

        public func upgrade(from : MU.MemShell<V1.Ledger.Mem>) : MU.MemShell<Mem> {
            MU.upgrade(
                from,
                func(a : V1.Ledger.Mem) : Mem {
                    {
                        reader = Reader.upgrade(a.reader);
                        sender = Sender.upgrade(a.sender);
                        accounts = a.accounts;
                        known_accounts = a.known_accounts;
                        var fee = a.fee;
                        var next_tx_id = a.next_tx_id;
                    };
                },
            );
        };
    };

    public module Reader {
        public func new() : MU.MemShell<Mem> = MU.new<Mem>({
            var last_indexed_tx = 0;
        });

        public type Mem = {
            var last_indexed_tx : Nat;
        };

        public func upgrade(from : MU.MemShell<V1.Reader.Mem>) : MU.MemShell<Mem> {
            MU.upgrade(from, func(a : V1.Reader.Mem) : Mem {
                {
                    var last_indexed_tx = a.last_indexed_tx;
                };
            });
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
            to : AccountMixed;
            from_subaccount : ?Blob;
            var created_at_time : Nat64; // 1000000000
            memo : Blob;
            var tries: Nat;
        };

        public func upgrade(from : MU.MemShell<V1.Sender.Mem>) : MU.MemShell<Mem> {
            MU.upgrade(from, func(a : V1.Sender.Mem) : Mem {
                let transformed = BTree.entries(a.transactions) |> Iter.map<(Blob, V1.Sender.Transaction), (Blob, Transaction)>(_,
                    func(e : (Blob, V1.Sender.Transaction)) : (Blob, Transaction) = (e.0, {
                        amount = e.1.amount;
                        to = #icrc(e.1.to);
                        from_subaccount = e.1.from_subaccount;
                        var created_at_time = e.1.created_at_time;
                        memo = e.1.memo;
                        var tries = e.1.tries;
                    }));

                {
                    transactions = BTree.fromArray<Blob, Transaction>(16, Blob.compare, Iter.toArray(transformed));
                    transaction_ids = a.transaction_ids;
                };
            });
        };
    };

    public type Account = { owner : Principal; subaccount : ?Blob };

    public type AccountMixed = {
        #icrc:Account;
        #icp:Blob;
    };
}
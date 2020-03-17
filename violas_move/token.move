address 0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614:

module ViolasToken {
    use 0x0::LibraAccount;
    use 0x0::Transaction;
    use 0x0::U64Util;
    use 0x0::AddressUtil;
    use 0x0::Vector;
    // use 0x0::LibraTransactionTimeout;
    // use 0x0::LibraCoin;
    // use 0x0::Hash;
    
    resource struct Owner {
    }
    
    resource struct T {
	value: u64,
    }

    resource struct UserInfo {
	magic: u64,
	token: address,
	violas_events: LibraAccount::EventHandle<ViolasEvent>,
	data: vector<u8>,
    }

    resource struct TokenInfo {
	owner: address,
	data: vector<u8>,
	bulletin_first: vector<u8>,
	bulletins: vector<vector<u8>>,
    }
    
    struct ViolasEvent {
	etype: u64,
	paras: vector<u8>,
	data:  vector<u8>,
    }

    fun require_published() {
	Transaction::assert(exists<T>(Transaction::sender()), 101);
    }

    fun require_owner() {
	Transaction::assert(exists<Owner>(Transaction::sender()), 102);
    }

    fun emit_events(etype: u64, paras: vector<u8>, data: vector<u8>) acquires UserInfo {
	let info = borrow_global_mut<UserInfo>(Transaction::sender());
	LibraAccount::emit_event<ViolasEvent>(&mut info.violas_events, ViolasEvent{ etype: etype, paras: paras, data: data});
    }

    public fun publish(userdata: vector<u8>, tokendata: vector<u8>) acquires UserInfo {
	let sender = Transaction::sender();
	
	Transaction::assert(!exists<T>(sender), 103);

	move_to_sender<T>(T{ value: 0 });

	move_to_sender<UserInfo>(UserInfo{
	    magic: 123456789,
	    token: 0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614,
	    violas_events: LibraAccount::new_event_handle<ViolasEvent>(),
	    data: *&userdata,
	});

	let data = userdata;
	Vector::append(&mut data, *&tokendata);
	emit_events(0, data, Vector::empty());
	
	if(sender == 0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614) {
	    move_to_sender<Owner>(Owner{});
	    move_to_sender<TokenInfo>(TokenInfo{ owner: sender, data: tokendata, bulletin_first: Vector::empty(), bulletins: Vector::empty() });
	}
    }

    fun value(coin_ref: &T): u64 {
    	coin_ref.value
    }
    
    fun deposit(payee: address, to_deposit: T) acquires T {
    	let T { value } = to_deposit;
    	let t = borrow_global_mut<T>(payee);
    	t.value = t.value + value;
    }

    fun withdraw(amount: u64) :T acquires T {
    	let t = borrow_global_mut<T>(Transaction::sender());
    	Transaction::assert(t.value >= amount, 104);
    	t.value = t.value - amount;
    	T{ value: amount }
    }

    fun pay_from_sender(payee: address, amount: u64) acquires T {
    	let t = withdraw(amount);
    	deposit(payee, t);
    }
    
    public fun mint(payee: address, amount: u64, data: vector<u8>) acquires T, UserInfo {
	require_published();
	require_owner();
	let t = T{ value: amount };
	deposit(payee, t);

	let v = AddressUtil::address_to_bytes(payee);
	Vector::append(&mut v, U64Util::u64_to_bytes(amount));
	Vector::append(&mut v, data);
	emit_events(1, v, Vector::empty());
    }

    public fun transfer(payee: address, amount: u64, data: vector<u8>) acquires T, UserInfo {
	require_published();
	pay_from_sender(payee, amount);
	
	let v = AddressUtil::address_to_bytes(payee);
	Vector::append(&mut v, U64Util::u64_to_bytes(amount));
	Vector::append(&mut v, data);
	emit_events(2, v, Vector::empty());
    }

    public fun record(data: vector<u8>) acquires UserInfo {
	emit_events(3, data, Vector::empty());
    }

    public fun move_owner(new_owner: address, data: vector<u8>) acquires TokenInfo, UserInfo {
	require_owner();
    	let token = borrow_global_mut<TokenInfo>(0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614);
	token.owner = new_owner;
	
	let v = AddressUtil::address_to_bytes(new_owner);
	Vector::append(&mut v, data);
	emit_events(4, v, Vector::empty());
    }

    public fun take_owner(owner: address, data: vector<u8>) acquires TokenInfo, Owner, UserInfo {
    	let token = borrow_global_mut<TokenInfo>(0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614);
	Transaction::assert(token.owner == Transaction::sender(), 105);

	let o = move_from<Owner>(owner);
	move_to_sender<Owner>(o);

	let v = AddressUtil::address_to_bytes(owner);
	Vector::append(&mut v, data);
	emit_events(5, v, Vector::empty());
    }

    public fun update_first_bulletin(data: vector<u8>) acquires TokenInfo, UserInfo {
	require_owner();
    	let token = borrow_global_mut<TokenInfo>(0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614);
	token.bulletin_first = *&data;
	emit_events(6, data, Vector::empty());
    }

    public fun append_bulletin(data: vector<u8>) acquires TokenInfo, UserInfo {
	require_owner();
    	let token = borrow_global_mut<TokenInfo>(0x7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614);
	Vector::push_back(&mut token.bulletins, *&data);
	emit_events(7, data, Vector::empty());
    }

    public fun destroy_owner(data: vector<u8>) acquires Owner, UserInfo{
	require_owner();
	Owner {} = move_from<Owner>(Transaction::sender());
	emit_events(8, data, Vector::empty());
    }

    public fun destroy_coin(amount: u64, data: vector<u8>) acquires T, UserInfo{
	require_owner();
	T { value: _ } = withdraw(amount);
	let v = U64Util::u64_to_bytes(amount);
	Vector::append(&mut v, data);
	emit_events(9, v, Vector::empty());
    }
}


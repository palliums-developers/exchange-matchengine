address 0x7257c2417e4d1038e1817c8f283ace2e {

module ViolasBank {
    use 0x1::Libra;
    use 0x1::LibraAccount;
    //use 0x1::Transaction;
    use 0x1::Event;
    use 0x1::Vector;
    use 0x1::LCS;
    use 0x1::LibraTimestamp;
    use 0x1::Debug;
    use 0x1::LibraBlock;
    use 0x1::Signer;
    
    resource struct LibraToken<Token> {
	coin: Libra::Libra<Token>,
	index: u64,
    }

    resource struct T {
	index: u64,
	value: u64,
    }

    resource struct BorrowInfo {
	principal: u64,
	interest_index: u64,
    }
    
    resource struct Tokens {
	ts: vector<T>,
	borrows: vector<BorrowInfo>,
    }

    resource struct Order {
	t: T,
	peer_token_idx: u64,
	peer_token_amount: u64,
    }
    
    resource struct UserInfo {
	violas_events: Event::EventHandle<ViolasEvent>,
	data: vector<u8>,
	orders: vector<Order>,
	order_freeslots: vector<u64>,
	debug: vector<u8>,
    }

    resource struct TokenInfo {
	currency_code: vector<u8>,
	owner: address,
	total_supply: u64,
	total_reserves: u64,
	total_borrows: u64,
	borrow_index: u64,
	price: u64,
	price_oracle: address,
	collateral_factor: u64,
	last_minute: u64,
	data: vector<u8>,
	bulletin_first: vector<u8>,
	bulletins: vector<vector<u8>>,
    }

    resource struct TokenInfoStore {
	supervisor: address,
	tokens: vector<TokenInfo>,
    }
    
    struct ViolasEvent {
	etype: u64,
	paras: vector<u8>,
	data:  vector<u8>,
    }

    ///////////////////////////////////////////////////////////////////////////////////
    
    fun new_mantissa(a: u64, b: u64) : u64 {
	let c = (a as u128) << 64;
	let d = (b as u128) << 32;
	let e = c / d;
	//assert(e != 0 || a == 0, 101);
	(e as u64)
    }
    
    fun mantissa_div(a: u64, b: u64) : u64 {
	let c = (a as u128) << 32;
	let d = c / (b as u128);
	(d as u64)
    }

    fun mantissa_mul(a: u64, b: u64) : u64 {
	let c = (a as u128) * (b as u128);
	let d = c >> 32;
	(d as u64)
    }

    fun safe_sub(a: u64, b: u64): u64 {
	if(a < b) { 0 } else { a - b }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    
    fun contract_address() : address {
	0x7257c2417e4d1038e1817c8f283ace2e
    }
    
    fun require_published(sender: address) {
	assert(exists<Tokens>(sender), 102);
    }

    fun require_supervisor(sender: address) acquires TokenInfoStore {
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	assert(sender == tokeninfos.supervisor, 103);
    }
    
    fun require_owner(sender: address, tokenidx: u64) acquires TokenInfoStore {
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(&tokeninfos.tokens, tokenidx);
	assert(ti.owner == sender, 104);
    }

    fun require_first_tokenidx(tokenidx: u64) {
	assert(tokenidx % 2 == 0, 105);
    }

    // fun require_second_tokenidx(tokenidx: u64) {
    // 	assert(tokenidx % 2 == 1, 106);
    // }

    fun require_price(tokenidx: u64) acquires TokenInfoStore {
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	assert(ti.price != 0, 107);
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    
    public fun zero(tokenidx: u64) : T {
    	T { index: tokenidx, value: 0 }
    }

    public fun value(coin_ref: &T): u64 {
    	coin_ref.value
    }
    
    public fun join(t1: T, t2: T) : T {
    	let T { index: i1, value: v1 } = t1;
    	let T { index: i2, value: v2 } = t2;
    	assert(i1 == i2, 108);
    	T { index: i1, value: v1+v2 }
    }

    public fun join2(t1: &mut T, t2: T) {
    	let T { index: i2, value: v2 } = t2;
    	assert(t1.index == i2, 109);
    	t1.value = t1.value + v2;
    }
    
    public fun split(t: &mut T, amount: u64) : T {
    	assert(t.value >= amount, 110);
    	t.value = safe_sub(t.value, amount);
    	T { index: t.index, value: amount }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    
    public fun balance_of(tokenidx: u64, account: address) : u64 acquires Tokens {
	let tokens = borrow_global<Tokens>(account);
	if(tokenidx < Vector::length(&tokens.ts)) {
	    let t = Vector::borrow(& tokens.ts, tokenidx);
	    t.value
	} else { 0 }
    }
    
    // public fun balance(tokenidx: u64) : u64 acquires Tokens {
    // 	balance_of(tokenidx, Transaction::sender())
    // }

    fun borrow_balance_of(tokenidx: u64, account: address) : u64 acquires Tokens, TokenInfoStore {
	// recentBorrowBalance = borrower.borrowBalance * market.borrowIndex / borrower.borrowIndex
	let tokens = borrow_global<Tokens>(account);
	let borrowinfo = Vector::borrow(& tokens.borrows, tokenidx);
	
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);

	//borrowinfo.principal * ti.borrow_index / borrowinfo.interest_index
	mantissa_div(mantissa_mul(borrowinfo.principal, ti.borrow_index), borrowinfo.interest_index)
    }

    // fun borrow_balance(tokenidx: u64) : u64 acquires Tokens, TokenInfoStore {
    // 	borrow_balance_of(tokenidx, Transaction::sender())
    // }
    
    public fun token_count() : u64 acquires TokenInfoStore {
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	Vector::length(&tokeninfos.tokens)
    }

    public fun total_supply(tokenidx: u64) : u64 acquires TokenInfoStore {
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	let token = Vector::borrow(& tokeninfos.tokens, tokenidx);
	token.total_supply
    }

    fun token_price(tokenidx: u64) : u64 acquires TokenInfoStore {
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	ti.price
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    
    fun deposit(payee: address, to_deposit: T) acquires TokenInfoStore,Tokens {
	extend_user_tokens(payee);
	let T { index, value } = to_deposit;
	let tokens = borrow_global_mut<Tokens>(payee);
	let t = Vector::borrow_mut(&mut tokens.ts, index);
	t.value = t.value + value; 
    }

    fun withdraw_from(tokenidx: u64, payer: address, amount: u64) : T acquires Tokens {
	let tokens = borrow_global_mut<Tokens>(payer);
	let t = Vector::borrow_mut(&mut tokens.ts, tokenidx);
	assert(t.value >= amount, 111);
	t.value = safe_sub(t.value, amount);
	T { index: tokenidx, value: amount }
    }
    
    fun pay_from(tokenidx: u64, payer: address, payee: address, amount: u64) acquires TokenInfoStore,Tokens {
	assert(payer != payee, 112);
    	let t = withdraw_from(tokenidx, payer, amount);
    	deposit(payee, t);
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    fun extend_user_tokens(payee: address) acquires TokenInfoStore, Tokens {
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let tokencnt = Vector::length(&tokeninfos.tokens);
	let tokens = borrow_global_mut<Tokens>(payee);
	let usercnt = Vector::length(&tokens.ts);
	loop {
	    if(usercnt >= tokencnt) break;
	    Vector::push_back(&mut tokens.ts, T{ index: usercnt, value: 0});
	    Vector::push_back(&mut tokens.borrows, BorrowInfo{ principal: 0, interest_index: new_mantissa(1,1)});
	    usercnt = usercnt + 1;
	}
    }

    fun emit_events(sender: address, etype: u64, paras: vector<u8>, data: vector<u8>) acquires UserInfo {
	let info = borrow_global_mut<UserInfo>(sender);
	Event::emit_event<ViolasEvent>(&mut info.violas_events, ViolasEvent{ etype: etype, paras: paras, data: data});
    }

    ///////////////////////////////////////////////////////////////////////////////////

    // fun debug(a: u64) acquires UserInfo {
    // 	let info = borrow_global_mut<UserInfo>(Transaction::sender());
    // 	if(a == 513) { // 0x0102...
    // 	    loop {
    // 	    	if(Vector::is_empty(&info.debug))
    // 	    	    break;
    // 	    	Vector::pop_back(&mut info.debug);
    // 	    };
    // 	};
    // 	Vector::append(&mut info.debug, LCS::to_bytes(&a));
    // }

    ///////////////////////////////////////////////////////////////////////////////////
    
    public fun publish(account: &signer, userdata: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	assert(!exists<Tokens>(sender), 113);
	move_to(account, Tokens{ ts: Vector::empty(), borrows: Vector::empty() });

	move_to(account, UserInfo{
	    violas_events: Event::new_event_handle<ViolasEvent>(account),
	    data: *&userdata,
	    orders: Vector::empty(),
	    order_freeslots: Vector::empty(),
	    debug: Vector::empty(),
	});

	if(sender == contract_address()) {
	    move_to(account, TokenInfoStore{ supervisor: contract_address(), tokens: Vector::empty() });
	};
	
	extend_user_tokens(sender);

	emit_events(sender, 0, userdata, Vector::empty());
    }

    public fun register_libra_token<CoinType>(account: &signer, price_oracle: address, collateral_factor: u64, tokendata: vector<u8>) : u64 acquires TokenInfoStore, Tokens, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_supervisor(sender);
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let len = Vector::length(&tokeninfos.tokens);
	move_to(account, LibraToken<CoinType> { coin: Libra::zero<CoinType>(), index: len });
	create_token(account, Libra::currency_code<CoinType>(), 0x0, price_oracle, collateral_factor, tokendata)
    }
    
    public fun create_token(account: &signer, currency_code: vector<u8>, owner: address, price_oracle: address, collateral_factor: u64, tokendata: vector<u8>) : u64 acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_supervisor(sender);

	let sender = Signer::address_of(account);

	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let len = Vector::length(&tokeninfos.tokens);
	let mantissa_one = new_mantissa(1, 1);

	Vector::push_back(&mut tokeninfos.tokens, TokenInfo {
	    currency_code: currency_code,
	    owner: owner,
	    total_supply: 0,
	    total_reserves: 0,
	    total_borrows: 0,
	    borrow_index: mantissa_one,
	    price: 0,
	    price_oracle: price_oracle,
	    collateral_factor: collateral_factor,
	    last_minute: LibraTimestamp::now_microseconds() / (60*1000*1000),
	    data: *&tokendata,
	    bulletin_first: Vector::empty(),
	    bulletins: Vector::empty()
	});
	Vector::push_back(&mut tokeninfos.tokens, TokenInfo {
	    currency_code: Vector::empty(),
	    owner: 0x0,
	    total_supply: 0,
	    total_reserves: 0,
	    total_borrows: 0,
	    borrow_index: mantissa_one,
	    price: 0,
	    price_oracle: price_oracle,
	    collateral_factor: collateral_factor,
	    last_minute: LibraTimestamp::now_microseconds() / (60*1000*1000),
	    data: *&tokendata,
	    bulletin_first: Vector::empty(),
	    bulletins: Vector::empty()
	});	

	extend_user_tokens(contract_address());

	let v = LCS::to_bytes(&owner);
	Vector::append(&mut v, tokendata);
	emit_events(sender, 1, v, LCS::to_bytes(&len));
	len
    }
    
    public fun mint(account: &signer, tokenidx: u64, payee: address, amount: u64, data: vector<u8>) acquires TokenInfoStore, Tokens, UserInfo {
	let sender = Signer::address_of(account);

	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_owner(sender, tokenidx);

 	extend_user_tokens(sender);
	extend_user_tokens(payee);
	
	let t = T{ index: tokenidx, value: amount };
	deposit(payee, t);

	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	ti.total_supply = ti.total_supply + amount;

	let v = LCS::to_bytes(&tokenidx);
	Vector::append(&mut v, LCS::to_bytes(&payee));
	Vector::append(&mut v, LCS::to_bytes(&amount));
	Vector::append(&mut v, data);
	emit_events(sender, 2, v, Vector::empty());
    }

    fun bank_mint(tokenidx: u64, payee: address, amount: u64) acquires TokenInfoStore, Tokens {
	let t = T{ index: tokenidx, value: amount };
	deposit(payee, t);
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	ti.total_supply = ti.total_supply + amount;
    }

    fun bank_burn( t: T) acquires TokenInfoStore {
	let T { index: tokenidx, value: amount } = t;
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	ti.total_supply = safe_sub(ti.total_supply, amount);
    }
    
    fun transfer_from(tokenidx: u64, payer: address, payee: address, amount: u64) acquires TokenInfoStore, Tokens {
	assert(payer != payee, 114);
	let t = withdraw_from(tokenidx, payer, amount);
	deposit(payee, t)
    }
    
    public fun transfer(account: &signer, tokenidx: u64, payee: address, amount: u64, data: vector<u8>) acquires TokenInfoStore, Tokens,  UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);

	extend_user_tokens(sender);
	extend_user_tokens(payee);

	pay_from(tokenidx, sender, payee, amount);
	
	let v = LCS::to_bytes(&tokenidx);
	Vector::append(&mut v, LCS::to_bytes(&payee));
	Vector::append(&mut v, LCS::to_bytes(&amount));
	Vector::append(&mut v, data);
	emit_events(sender, 3, v, Vector::empty());
    }

    // public fun move_owner(tokenidx: u64, new_owner: address, data: vector<u8>) acquires TokenInfoStore, UserInfo {
    // 	require_published();
    // 	require_first_tokenidx(tokenidx);
    // 	require_owner(tokenidx);
    // 	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    // 	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    // 	ti.owner = new_owner;

    // 	let v = LCS::to_bytes(&tokenidx);
    // 	Vector::append(&mut v, LCS::to_bytes(&new_owner));
    // 	Vector::append(&mut v, data);
    // 	emit_events(4, v, Vector::empty());
    // }

    // public fun move_supervisor(new_supervisor: address, data: vector<u8>) acquires TokenInfoStore, UserInfo {
    // 	require_published();
    // 	require_supervisor();
    // 	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    // 	tokeninfos.supervisor = new_supervisor;

    // 	let v = LCS::to_bytes(&new_supervisor);
    // 	Vector::append(&mut v, data);
    // 	emit_events(5, v, Vector::empty());
    // }
    
    ///////////////////////////////////////////////////////////////////////////////////

    fun exchange_rate(tokenidx: u64) : u64 acquires Tokens, TokenInfoStore {
	let tokens = borrow_global<Tokens>(contract_address());
	let t = Vector::borrow(& tokens.ts, tokenidx);
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	let ti1 = Vector::borrow(& tokeninfos.tokens, tokenidx+1);
	
	if(ti1.total_supply == 0) {
	    new_mantissa(1, 100)
	} else {
	    // exchangeRate = (totalCash + totalBorrows - totalReserves) / totalSupply
	    new_mantissa(t.value+ti.total_borrows-ti.total_reserves, ti1.total_supply)
	}
    }

    fun borrow_rate(tokenidx: u64) : u64 acquires Tokens, TokenInfoStore {
	let tokens = borrow_global<Tokens>(contract_address());
	let t = Vector::borrow(& tokens.ts, tokenidx);
	let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	
	// utilization rate of the market: `borrows / (cash + borrows - reserves)`
	let util = 
	if(ti.total_borrows == 0) {
	    0
	} else {
	    new_mantissa(ti.total_borrows, ti.total_borrows + safe_sub(t.value, ti.total_reserves))
	};
	let baserate_perminute = new_mantissa(5*60*24*30, 100*60*24*365);
	baserate_perminute + mantissa_mul(baserate_perminute, util)
    }

    fun accrue_interest(tokenidx: u64) acquires Tokens, TokenInfoStore {
	let borrowrate = borrow_rate(tokenidx);
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);

	let minute = LibraTimestamp::now_microseconds() / (60*1000*1000);
	let cnt = safe_sub(minute, ti.last_minute);
	borrowrate = borrowrate*cnt;
	ti.last_minute = minute;
	
	let interest_accumulated = mantissa_mul(ti.total_borrows, borrowrate);
	ti.total_borrows = ti.total_borrows + interest_accumulated;

	let reserve_factor = new_mantissa(1, 20);
	ti.total_reserves = ti.total_reserves + mantissa_mul(interest_accumulated, reserve_factor);

	ti.borrow_index = ti.borrow_index + mantissa_mul(ti.borrow_index, borrowrate);
    }

    fun bank_token_2_base(amount: u64, exchange_rate: u64, collateral_factor: u64, price: u64) : u64 {
	let value = mantissa_mul(amount, exchange_rate);
	value = mantissa_mul(value, collateral_factor);
	value = mantissa_mul(value, price);
	value
    }

    fun account_liquidity(account: address, modify_tokenidx: u64, redeem_tokens: u64, borrow_amount: u64) : (u64, u64) acquires Tokens, TokenInfoStore {
	let len = token_count();
	let i = 0;
	let sum_collateral = 0;
	let sum_borrow = 0;
	
	loop {
	    if(i == len) break;

	    let balance = balance_of(i+1, account);
	    let exchange_rate = exchange_rate(i);
	    let borrow_balance = borrow_balance_of(i, account);
	    
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow(& tokeninfos.tokens, i);
	    
	    sum_collateral = sum_collateral + bank_token_2_base(balance, exchange_rate, ti.collateral_factor, ti.price);

	    sum_borrow = sum_borrow + mantissa_mul(borrow_balance, ti.price);

	    if(i == modify_tokenidx) {
		if(redeem_tokens > 0) {
		    sum_borrow = sum_borrow + bank_token_2_base(redeem_tokens, exchange_rate, ti.collateral_factor, ti.price);
		};
		if(borrow_amount > 0) {
		    sum_borrow = sum_borrow + mantissa_mul(borrow_amount, ti.price);
		};
	    };
	    
	    i = i + 2;
	};
	
	(sum_collateral, sum_borrow)
    }
    
    ///////////////////////////////////////////////////////////////////////////////////


    public fun update_price<CoinType>(account: &signer, price: u64) acquires TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	update_price_index(account, libratoken.index, price);
    }
    
    public fun update_price_index(account: &signer, tokenidx: u64, price: u64) acquires TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	assert(ti.price_oracle == sender, 116);
	ti.price = price;

	let v = LCS::to_bytes(&tokenidx);
	Vector::append(&mut v, LCS::to_bytes(&price));
	emit_events(sender, 6, v, Vector::empty());
    }

    public fun lock<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	lock_index(account, libratoken.index, amount, data);
    }
    
    public fun lock_index(account: &signer, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_price(tokenidx);
	
	extend_user_tokens(sender);

	accrue_interest(tokenidx);
	let er = exchange_rate(tokenidx);
	pay_from(tokenidx, sender, contract_address(), amount);

	let tokens = mantissa_div(amount, er);
	bank_mint(tokenidx+1, sender, tokens);

	let v = LCS::to_bytes(&tokenidx);
 	Vector::append(&mut v, LCS::to_bytes(&amount));
 	Vector::append(&mut v, data);
	emit_events(sender, 7, v, Vector::empty());
    }

    public fun redeem<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	redeem_index(account, libratoken.index, amount, data);
    }
    
    public fun redeem_index(account: &signer, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);

	Debug::print(&LibraBlock::get_current_block_height());
	
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_price(tokenidx);

	extend_user_tokens(sender);

	accrue_interest(tokenidx);

	let er = exchange_rate(tokenidx);

	let token_amount = mantissa_div(amount, er);
	if(amount == 0) {
	    token_amount = balance_of(tokenidx+1, sender);
	    amount = mantissa_mul(token_amount, er);
	};

	let (sum_collateral, sum_borrow) = account_liquidity(sender, tokenidx, token_amount, 0);

	assert(sum_collateral+1000000 >= sum_borrow, 117);

	let T{ index:_, value:_ } = withdraw_from(tokenidx+1, sender, token_amount);	
	
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti1 = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx+1);
	ti1.total_supply = safe_sub(ti1.total_supply, token_amount);

	transfer_from(tokenidx, contract_address(), sender, amount);

	let v = LCS::to_bytes(&tokenidx);
 	Vector::append(&mut v, LCS::to_bytes(&amount));
 	Vector::append(&mut v, data);
	emit_events(sender, 8, v, Vector::empty());
    }

    public fun borrow<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	borrow_index(account, libratoken.index, amount, data);
    }
    
    public fun borrow_index(account: &signer, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_price(tokenidx);

	extend_user_tokens(sender);

	accrue_interest(tokenidx);

	let (sum_collateral, sum_borrow) = account_liquidity(sender, tokenidx, 0, amount);
	assert(sum_collateral >= sum_borrow, 118);

	let balance = borrow_balance_of(tokenidx, sender);

	let tokens = borrow_global_mut<Tokens>(sender);
	let borrowinfo = Vector::borrow_mut(&mut tokens.borrows, tokenidx);

	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);

	ti.total_borrows = ti.total_borrows + amount;
	borrowinfo.principal = balance + amount;
	borrowinfo.interest_index = ti.borrow_index;

	transfer_from(tokenidx, contract_address(), sender, amount);

	let v = LCS::to_bytes(&tokenidx);
 	Vector::append(&mut v, LCS::to_bytes(&amount));
 	Vector::append(&mut v, data);
	emit_events(sender, 9, v, Vector::empty());
    }

    fun repay_borrow_for(sender: address, tokenidx: u64, borrower: address, amount: u64) acquires Tokens, TokenInfoStore {
	let balance = borrow_balance_of(tokenidx, borrower);
	assert(amount <= balance, 119);
	if(amount == 0) { amount = balance; };

	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	ti.total_borrows = safe_sub(ti.total_borrows, amount);

	let tokens = borrow_global_mut<Tokens>(borrower);
	let borrowinfo = Vector::borrow_mut(&mut tokens.borrows, tokenidx);
	borrowinfo.principal = safe_sub(balance, amount);
	borrowinfo.interest_index = ti.borrow_index;
	    
	pay_from(tokenidx, sender, contract_address(), amount);
    }

    public fun repay_borrow<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	repay_borrow_index(account, libratoken.index, amount, data);
    }
    
    public fun repay_borrow_index(account: &signer, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_price(tokenidx);

	extend_user_tokens(sender);
	
	accrue_interest(tokenidx);
	repay_borrow_for(sender, tokenidx, sender, amount);

	let v = LCS::to_bytes(&tokenidx);
 	Vector::append(&mut v, LCS::to_bytes(&amount));
 	Vector::append(&mut v, data);
	emit_events(sender, 10, v, Vector::empty());
    }

    public fun liquidate_borrow<CoinType1, CoinType2>(account: &signer, borrower: address, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, LibraToken {
	let libratoken1 = borrow_global<LibraToken<CoinType1>>(contract_address());
	let libratoken2 = borrow_global<LibraToken<CoinType2>>(contract_address());
	liquidate_borrow_index(account, libratoken1.index, borrower, amount, libratoken2.index, data);
    }
    
    public fun liquidate_borrow_index(account: &signer, tokenidx: u64, borrower: address, amount: u64, collateral_tokenidx: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_first_tokenidx(collateral_tokenidx);
	require_price(tokenidx);
	require_price(collateral_tokenidx);

	extend_user_tokens(sender);
	extend_user_tokens(borrower);
	
	accrue_interest(tokenidx);

	let (sum_collateral, sum_borrow) = account_liquidity(borrower, 99999, 0, 0);
	assert(sum_collateral < sum_borrow, 120);

	let borrowed = borrow_balance_of(tokenidx, borrower);
	assert(amount <= borrowed, 121);

	if(amount == 0) { amount = borrowed; };

	let price0 = token_price(tokenidx);
	let price1 = token_price(collateral_tokenidx);

	let base_amount = mantissa_mul(amount, price0);
	assert(base_amount <= safe_sub(sum_borrow, sum_collateral), 122);
	
	repay_borrow_for(sender, tokenidx, borrower, amount);

	// amount1 * price1 = amount2 * exchange_rate2 * price2
	let value = mantissa_mul(amount, price0);
	value = mantissa_div(value, exchange_rate(collateral_tokenidx));
	value = mantissa_div(value, price1);
	value = value + mantissa_mul(value, new_mantissa(1, 10));

	transfer_from(collateral_tokenidx+1, borrower, sender, value);

	let v = LCS::to_bytes(&tokenidx);
 	Vector::append(&mut v, LCS::to_bytes(&borrower));
 	Vector::append(&mut v, LCS::to_bytes(&amount));
 	Vector::append(&mut v, LCS::to_bytes(&collateral_tokenidx));
 	Vector::append(&mut v, data);
	emit_events(sender, 11, v, Vector::empty());
    }

    public fun update_collateral_factor<CoinType>(account: &signer, factor: u64) acquires TokenInfoStore, UserInfo, LibraToken {
	let libratoken = borrow_global<LibraToken<CoinType>>(contract_address());
	update_collateral_factor_index(account, libratoken.index, factor);
    }

    public fun update_collateral_factor_index(account: &signer, tokenidx: u64, factor: u64) acquires TokenInfoStore, UserInfo {
	let sender = Signer::address_of(account);
	require_published(sender);
	require_first_tokenidx(tokenidx);
	require_supervisor(sender);
	
	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	ti.collateral_factor = factor;

	let v = LCS::to_bytes(&tokenidx);
	Vector::append(&mut v, LCS::to_bytes(&factor));
	emit_events(sender, 12, v, Vector::empty());
    }

    public fun enter_bank<CoinType>(account: &signer, amount: u64) acquires LibraToken, TokenInfoStore, Tokens, UserInfo {
	let sender = Signer::address_of(account);
	let payer_withdrawal_cap = LibraAccount::extract_withdraw_capability(account);
	let to_deposit = LibraAccount::withdraw_from<CoinType>(&payer_withdrawal_cap, amount);
	LibraAccount::restore_withdraw_capability(payer_withdrawal_cap);
	
	let libratoken = borrow_global_mut<LibraToken<CoinType>>(contract_address());
	Libra::deposit(&mut libratoken.coin, to_deposit);
	bank_mint(libratoken.index, sender, amount);

	let v = LCS::to_bytes(&libratoken.index);
	Vector::append(&mut v, LCS::to_bytes(&amount));
	emit_events(sender, 13, v, Vector::empty());
    }

    public fun exit_bank<CoinType>(account: &signer, amount: u64) acquires LibraToken, TokenInfoStore, Tokens, UserInfo {
	let sender = Signer::address_of(account);
	let libratoken = borrow_global_mut<LibraToken<CoinType>>(contract_address());
	let to_deposit = Libra::withdraw(&mut libratoken.coin, amount);
	LibraAccount::deposit_to(account, to_deposit);
	let t = withdraw_from(libratoken.index, sender, amount);
	bank_burn(t);

	let v = LCS::to_bytes(&libratoken.index);
	Vector::append(&mut v, LCS::to_bytes(&amount));
	emit_events(sender, 14, v, Vector::empty());
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    
    // public fun make_order(idxa: u64, amounta: u64, idxb: u64, amountb: u64, data: vector<u8>) : u64 acquires Tokens, UserInfo {
    // 	require_published();
    // 	assert(amounta > 0, 123);

    // 	let t = withdraw(idxa, amounta);
    // 	let info = borrow_global_mut<UserInfo>(Transaction::sender());
    // 	let len = Vector::length(&info.orders);
    // 	let idx = len;

    // 	Vector::push_back(&mut info.orders, Order { t: t, peer_token_idx: idxb, peer_token_amount: amountb});	    
	
    // 	if(!Vector::is_empty(&info.order_freeslots)) {
    // 	    idx = Vector::pop_back(&mut info.order_freeslots);
    // 	    let order = Vector::swap_remove(&mut info.orders, idx);
    // 	    let Order { t: T { index:_, value:_ }, peer_token_idx:_, peer_token_amount:_ } = order;
    // 	};
	
    // 	let v = LCS::to_bytes(&idxa);
    // 	Vector::append(&mut v, LCS::to_bytes(&amounta));
    // 	Vector::append(&mut v, LCS::to_bytes(&idxb));
    // 	Vector::append(&mut v, LCS::to_bytes(&amountb));
    // 	Vector::append(&mut v, data);
    // 	emit_events(4, v, LCS::to_bytes(&idx));

    // 	idx
    // }

    // public fun cancel_order(orderidx: u64, idxa: u64, amounta: u64, idxb: u64, amountb: u64, data: vector<u8>) acquires TokenInfoStore,Tokens, UserInfo {
    // 	require_published();
    // 	let info = borrow_global_mut<UserInfo>(Transaction::sender());
	
    // 	Vector::push_back(&mut info.orders, Order { t: T{ index: 0, value: 0}, peer_token_idx: 0, peer_token_amount: 0});	    
    // 	Vector::push_back(&mut info.order_freeslots, orderidx);
    // 	let order = Vector::swap_remove(&mut info.orders, orderidx);
	
    // 	assert(order.t.index == idxa, 107);
    // 	assert(order.t.value == amounta, 108);
    // 	assert(order.peer_token_idx == idxb, 109);
    // 	assert(order.peer_token_amount == amountb, 110);
	
    // 	let Order { t: t, peer_token_idx:_, peer_token_amount:_ } = order;
    // 	deposit(Transaction::sender(), t);
	
    // 	let v = LCS::to_bytes(&idxa);
    // 	Vector::append(&mut v, LCS::to_bytes(&amounta));
    // 	Vector::append(&mut v, LCS::to_bytes(&idxb));
    // 	Vector::append(&mut v, LCS::to_bytes(&amountb));
    // 	Vector::append(&mut v, data);
    // 	emit_events(5, v, Vector::empty());
    // }
    
    // public fun take_order(maker: address, orderidx: u64, idxa: u64, amounta: u64, idxb: u64, amountb: u64, data: vector<u8>) acquires TokenInfoStore, Tokens, UserInfo {
    // 	require_published();
    // 	let info = borrow_global_mut<UserInfo>(maker);
    // 	let len = Vector::length(&info.orders);
    // 	Vector::push_back(&mut info.orders, Order { t: T {index: idxa, value: 0}, peer_token_idx: idxb, peer_token_amount: amountb });

    // 	let order = Vector::swap_remove(&mut info.orders, orderidx);
	
    // 	assert(order.t.index == idxa, 111);
    // 	assert(order.t.value == amounta, 112);
    // 	assert(order.peer_token_idx == idxb, 113);
    // 	assert(order.peer_token_amount == amountb, 114);

    // 	pay_from_sender(idxb, maker, amountb);
    // 	let Order { t: t, peer_token_idx:_, peer_token_amount:_ } = order;
    // 	deposit(Transaction::sender(), t );

	
    // 	if(len == orderidx+1) {
    // 	    let o = Vector::pop_back(&mut info.orders);
    // 	    let Order { t: T { index:_, value:_ }, peer_token_idx:_, peer_token_amount:_ } = o;
    // 	} else {
    // 	    Vector::push_back(&mut info.order_freeslots, orderidx);
    // 	};

    // 	let v = LCS::to_bytes(&maker);
    // 	Vector::append(&mut v, LCS::to_bytes(&orderidx));
    // 	Vector::append(&mut v, LCS::to_bytes(&idxa));
    // 	Vector::append(&mut v, LCS::to_bytes(&amounta));
    // 	Vector::append(&mut v, LCS::to_bytes(&idxb));
    // 	Vector::append(&mut v, LCS::to_bytes(&amountb));
    // 	Vector::append(&mut v, data);
    // 	emit_events(6, v, Vector::empty());
    // }
    
    // public fun update_first_bulletin(tokenidx: u64, data: vector<u8>) acquires TokenInfoStore, UserInfo {
    // 	require_published();
    // 	require_owner(tokenidx);
    // 	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    // 	let token = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    // 	token.bulletin_first = *&data;
	
    // 	let v = LCS::to_bytes(&tokenidx);
    // 	Vector::append(&mut v, data);
    // 	emit_events(8, v, Vector::empty());
    // }

    // public fun append_bulletin(tokenidx: u64, data: vector<u8>) acquires TokenInfoStore, UserInfo {
    // 	require_published();
    // 	require_owner(tokenidx);
    // 	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    // 	let token = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    // 	Vector::push_back(&mut token.bulletins, *&data);

    // 	let v = LCS::to_bytes(&tokenidx);
    // 	Vector::append(&mut v, data);
    // 	emit_events(9, v, Vector::empty());
    // }

    // public fun destroy_owner(tokenidx: u64, data: vector<u8>) acquires TokenInfoStore, UserInfo{
    // 	require_published();
    // 	require_owner(tokenidx);
    // 	let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    // 	let token = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    // 	token.owner = 0x0;

    // 	let v = LCS::to_bytes(&tokenidx);
    // 	Vector::append(&mut v, data);
    // 	emit_events(10, v, Vector::empty());
    // }
    
    // public fun destroy_coin(tokenidx: u64, amount: u64, data: vector<u8>) acquires TokenInfoStore, Tokens, UserInfo{
    // 	require_published();
    // 	require_owner(tokenidx);
    // 	T { index: _, value: _ } = withdraw(tokenidx, amount);
	
    // 	let v = LCS::to_bytes(&tokenidx);
    // 	Vector::append(&mut v, LCS::to_bytes(&amount));
    // 	Vector::append(&mut v, data);
    // 	emit_events(11, v, Vector::empty());
    // }

    // public fun record(data: vector<u8>) acquires UserInfo {
    // 	require_published();
    // 	emit_events(12, data, Vector::empty());
    // }
}

}

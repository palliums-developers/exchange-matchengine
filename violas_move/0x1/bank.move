address 0x1 {
    
    module ViolasBank {
	use 0x1::Diem;
	use 0x1::DiemAccount;
	use 0x1::Event;
	use 0x1::Vector;
	use 0x1::BCS;
	use 0x1::DiemTimestamp;
	//use 0x1::Debug;
	//use 0x1::DiemBlock;
	use 0x1::Signer;
	use 0x1::Option::{Self, Option};
	use 0x1::FixedPoint32;
	use 0x1::Oracle;
	use 0x1::VLS::VLS;
	
	//use 0x1::ViolasBank as PBank;
	
	resource struct DiemToken<Token> {
	    coin: Diem::Diem<Token>,
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
	    last_exchange_rates: vector<u64>,
	    incentive_supply_indexes: vector<u64>,
	    incentive_borrow_indexes: vector<u64>,
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
	    base_rate: u64,
	    rate_multiplier: u64,
	    rate_jump_multiplier: u64,
	    rate_kink: u64,
	    last_minute: u64,

	    incentive_supply_index: u64,
	    incentive_supply_timestamp: u64,
	    incentive_speed: u64,
	    incentive_borrow_index: u64,
	    incentive_borrow_timestamp: u64,

	    price_oracle_last_timestamp: u64,
	    
	    data: vector<u8>,
	    bulletin_first: vector<u8>,
	    bulletins: vector<vector<u8>>,
	}

	resource struct TokenInfoStore {
	    supervisor: address,
	    tokens: vector<TokenInfo>,
	    withdraw_capability: Option<DiemAccount::WithdrawCapability>,
	    disabled: bool,
	    migrated: bool,
	    version: u64,
	    incentive_rate: u64,
	    incentive_refresh_speeds_last_minute: u64,
	    incentive_rate_last_minute: u64,
	}
	
	struct ViolasEvent {
	    etype: u64,
	    timestamp: u64,
	    paras: vector<u8>,
	    data:  vector<u8>,
	}

	struct EventPublish {
	    userdata: vector<u8>,
	}

	struct EventRegisterDiemToken {
	    currency_code: vector<u8>,
	    price_oracle: address,
	    collateral_factor: u64,
	    base_rate: u64,
	    rate_multiplier: u64,
	    rate_jump_multiplier: u64,
	    rate_kink: u64,
	    tokendata: vector<u8>,
	}

	struct EventMint {
	    tokenidx: u64,
	    payee: address,
	    amount: u64,
	    data: vector<u8>,
	}

	struct EventTransfer {
	    tokenidx: u64,
	    payee: address,
	    amount: u64,
	    data: vector<u8>,
	}

	struct EventUpdatePrice {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    price: u64,
	}

	struct EventUpdatePriceFromOracle {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    price: u64,
	}

	struct EventLock {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	    data: vector<u8>,
	    incentive: u64,
	}

	struct EventRedeem {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	    data: vector<u8>,
	    incentive: u64,
	}

	struct EventBorrow {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	    data: vector<u8>,
	    incentive: u64,
	}

	struct EventRepayBorrow {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	    data: vector<u8>,
	    incentive: u64,
	}

	struct EventLiquidateBorrow {
	    currency_code1: vector<u8>,
	    currency_code2: vector<u8>,
	    tokenidx: u64,
	    borrower: address,
	    amount: u64,
	    collateral_tokenidx: u64,
	    collateral_amount: u64,
	    data: vector<u8>,	    
	}

	struct EventUpdateCollateralFactor {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    factor: u64,
	}

	struct EventUpdateRateModel {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    base_rate: u64,
	    rate_multiplier: u64,
	    rate_jump_multiplier: u64,
	    rate_kink: u64,
	}
	
	struct EventEnterBank {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	}

	struct EventExitBank {
	    currency_code: vector<u8>,
	    tokenidx: u64,
	    amount: u64,
	}

	struct EventSetIncentiveRate {
	    rate: u64,
	}

	struct EventClaimIncentive {
	    incentive: u64,
	}

	///////////////////////////////////////////////////////////////////////////////////
	
	public fun set_incentive_rate(account: &signer, rate: u64) acquires TokenInfoStore, Tokens, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    // require_supervisor(sender);
	    assert(sender == 0x000000000000000000000000564c5301, 501);

	    check_for_incentive_speeds_refresh();
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    if(tokeninfos.incentive_rate_last_minute > 0) {
		let delta_minutes = safe_sub(DiemTimestamp::now_microseconds()/(60*1000*1000), tokeninfos.incentive_rate_last_minute);
		assert(delta_minutes > 23*60, 602);
	    };
	    tokeninfos.incentive_rate = rate/(2*24*60);
	    tokeninfos.incentive_rate_last_minute = DiemTimestamp::now_microseconds() / (60*1000*1000);
	    
    	    let withdraw_capability = DiemAccount::extract_withdraw_capability(account);
    	    DiemAccount::pay_from<VLS>(&withdraw_capability, contract_address(), rate, Vector::empty(), Vector::empty());
    	    DiemAccount::restore_withdraw_capability(withdraw_capability);
	    
	    refresh_incentive_speeds();
    	    let input = EventSetIncentiveRate {
    		rate: rate,
    	    };
    	    emit_events(account, 17, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	fun check_for_incentive_speeds_refresh() acquires TokenInfoStore {
	    let now = DiemTimestamp::now_microseconds() / (60*1000*1000);
    	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());

	    let delta = safe_sub(now, tokeninfos.incentive_refresh_speeds_last_minute);
	    let delta1 = 0;
	    if(tokeninfos.incentive_rate_last_minute > 0) {
		delta1 = safe_sub(now, tokeninfos.incentive_rate_last_minute);
	    };
	    
	    if(delta > 10) { 
		refresh_incentive_speeds();
    		let tokeninfos1 = borrow_global_mut<TokenInfoStore>(contract_address());
		tokeninfos1.incentive_refresh_speeds_last_minute = now;
	    };

	    if(delta1 > 24*60) {
    		let tokeninfos1 = borrow_global_mut<TokenInfoStore>(contract_address());
		tokeninfos1.incentive_rate = 0;
		tokeninfos1.incentive_rate_last_minute = 0;
		refresh_incentive_speeds();
	    }
	}
	
	fun refresh_incentive_speeds() acquires TokenInfoStore {
    	    let len = token_count();
    	    let i = 0;
    	    loop {
    		if(i == len) break;
		update_incentive_supply_index(i);
		update_incentive_borrow_index(i);
		i = i + 2;
	    };

	    let utilities : vector<u64> = Vector::empty();
	    let total_utility =  0;
	    i = 0;
	    loop {
    		if(i == len) break;
		let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
		let ti = Vector::borrow(& tokeninfos.tokens, i);
		let utility = mantissa_mul(ti.total_borrows, token_price(i));
		Vector::push_back(&mut utilities, utility);
		total_utility = total_utility + utility; 
		i = i + 2;
	    };

	    i = 0;
	    loop {
    		if(i == len) break;
		let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
		let utility = Vector::borrow(&utilities, i/2);
		let ti = Vector::borrow_mut(&mut tokeninfos.tokens, i);
		ti.incentive_speed = 0;
		if (total_utility > 0) {
		    ti.incentive_speed = mantissa_mul(tokeninfos.incentive_rate, mantissa_div(*utility, total_utility))
		};
		i = i + 2;
	    };

	}

	fun update_incentive_supply_index(tokenidx: u64) acquires TokenInfoStore {
	    let total_supply = total_supply(tokenidx+1);
	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	    let now = DiemTimestamp::now_microseconds() / (60*1000*1000);
	    let delta_minutes = safe_sub(now, ti.incentive_supply_timestamp);
	    if(delta_minutes > 0) {
		let accured = ti.incentive_speed * delta_minutes;
		let ratio = 0;
		if (total_supply > 0) {
		    ratio = mantissa_div(accured, total_supply);
		};
		ti.incentive_supply_index = ti.incentive_supply_index + ratio;
		ti.incentive_supply_timestamp = now;
	    }
	}

	fun update_incentive_borrow_index(tokenidx: u64) acquires TokenInfoStore {
	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
	    let now = DiemTimestamp::now_microseconds() / (60*1000*1000);
	    let delta_minutes = safe_sub(now, ti.incentive_borrow_timestamp);
	    if(delta_minutes > 0) {
		let borrow_amount = mantissa_div(ti.total_borrows, ti.borrow_index);
		let accured = ti.incentive_speed * delta_minutes;
		let ratio = 0;
		if (borrow_amount > 0) {
		    ratio = mantissa_div(accured, borrow_amount);
		};
		ti.incentive_borrow_index = ti.incentive_borrow_index + ratio;
		ti.incentive_borrow_timestamp = now;
	    }
	}
	
	fun distribute_supply_incentive(tokenidx: u64, sender: address) : u64 acquires TokenInfoStore, Tokens {
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);

	    let supply = balance_of_index(tokenidx+1, sender);
	    let tokens = borrow_global_mut<Tokens>(sender);
    	    let index = Vector::borrow_mut(&mut tokens.incentive_supply_indexes, tokenidx);
	    
	    let delta_index = safe_sub(ti.incentive_supply_index, *index);
	    let delta_vls = mantissa_mul(supply, delta_index);

	    if(delta_vls > 0 && DiemAccount::balance<VLS>(contract_address()) > delta_vls) {
    		DiemAccount::pay_from<VLS>(Option::borrow(&tokeninfos.withdraw_capability), sender, delta_vls, Vector::empty(), Vector::empty());
	    } else { delta_vls = 0; };

	    *index = ti.incentive_supply_index;
	    delta_vls
	}

	fun distribute_borrow_incentive(tokenidx: u64, sender: address) : u64 acquires TokenInfoStore, Tokens {
	    let borrow_balance = borrow_balance_of_index(tokenidx, sender);
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	    
	    let borrow = mantissa_div(borrow_balance, ti.borrow_index);
	    let tokens = borrow_global_mut<Tokens>(sender);
    	    let index = Vector::borrow_mut(&mut tokens.incentive_borrow_indexes, tokenidx);
	    
	    let delta_index = safe_sub(ti.incentive_borrow_index, *index);
	    let delta_vls = mantissa_mul(borrow, delta_index);

	    if(delta_vls > 0 && DiemAccount::balance<VLS>(contract_address()) > delta_vls) {
    		DiemAccount::pay_from<VLS>(Option::borrow(&tokeninfos.withdraw_capability), sender, delta_vls, Vector::empty(), Vector::empty());
	    } else { delta_vls = 0; };

	    *index = ti.incentive_borrow_index;
	    delta_vls
	}


	public fun claim_incentive(account: &signer) acquires TokenInfoStore, Tokens, UserInfo {
    	    let sender = Signer::address_of(account);	
	    let total = 0;
    	    let len = token_count();
    	    let i = 0;
    	    loop {
    		if(i == len) break;
    		update_incentive_supply_index(i);
    		let v1 = distribute_supply_incentive(i, sender);
    		update_incentive_borrow_index(i);
    		let v2 = distribute_borrow_incentive(i, sender);
		total = total + v1 + v2;
		i = i + 2;
	    };
	    
    	    let input = EventClaimIncentive {
		incentive: total,
    	    };
	    
    	    emit_events(account, 18, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
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
	    0x00000000000000000000000042414e4b
	}

	public fun version() :u64 { 1 }
	
	fun require_published(sender: address) {
	    assert(exists<Tokens>(sender), 102);
	}

	fun require_supervisor(sender: address) acquires TokenInfoStore {
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    assert(sender == tokeninfos.supervisor, 103);
	}

	fun require_enabled() acquires TokenInfoStore {
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    assert(false == tokeninfos.disabled, 1030);
	}
	
	fun require_owner(sender: address, tokenidx: u64) acquires TokenInfoStore {
	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow(&tokeninfos.tokens, tokenidx);
	    assert(ti.owner == sender, 104);
	}

	fun require_first_tokenidx(tokenidx: u64) {
	    assert(tokenidx % 2 == 0, 105);
	}

	fun require_price(tokenidx: u64) acquires TokenInfoStore {
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);
	    assert(ti.price != 0, 107);
	}
	
	///////////////////////////////////////////////////////////////////////////////////

	//public fun migrate_data(account: &signer) acquires Tokens, TokenInfoStore {
	public fun migrate_data(account: &signer) acquires TokenInfoStore {
	    let sender = Signer::address_of(account);
	    require_supervisor(sender);
	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	    assert(tokeninfos.migrated == false, 1070);
	    
	    migrate_data_impl(account);
	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    tokeninfos.migrated = true;
	}

	fun migrate_data_impl(_account: &signer) {}
	
	// fun migrate_data_impl(account: &signer) acquires Tokens, TokenInfoStore {
	// 	let sender = Signer::address_of(account);
	// 	let len = token_count();
	// 	let i = 0;
	// 	loop {
	// 	    if(i == len) break;
	
	// 	    let (value, principal, interest_index) = PBank::balance_and_borrow(account, i);

	// 	    let tokens = borrow_global_mut<Tokens>(sender);
	// 	    let t = Vector::borrow_mut(&mut tokens.ts, i);
	// 	    t.value = value;
	// 	    let borrowinfo = Vector::borrow_mut(&mut tokens.borrows, i);
	// 	    borrowinfo.principal = principal;
	// 	    borrowinfo.interest_index = interest_index;

	// 	    if(sender == contract_address()) {
	// 		let (total_supply, total_reserves, total_borrows, borrow_index) = PBank::token_info(account, i);
	// 		let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	// 		let token = Vector::borrow_mut(&mut tokeninfos.tokens, i);
	// 		token.total_supply = total_supply;
	// 		token.total_reserves = total_reserves;
	// 		token.total_borrows = total_borrows;
	// 		token.borrow_index = borrow_index;
	// 	    };
	
	// 	    i = i + 1;
	// 	}
	// }
	
	
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

	public fun split(t: &mut T, amount: u64) : T {
    	    assert(t.value >= amount, 110);
    	    t.value = safe_sub(t.value, amount);
    	    T { index: t.index, value: amount }
	}

	///////////////////////////////////////////////////////////////////////////////////
	
	public fun balance_and_borrow(account: &signer, tokenidx: u64): (u64, u64, u64) acquires Tokens {
    	    let sender = Signer::address_of(account);
    	    let tokens = borrow_global<Tokens>(sender);
    	    let t = Vector::borrow(& tokens.ts, tokenidx);
    	    let borrowinfo = Vector::borrow(& tokens.borrows, tokenidx);
    	    (t.value, borrowinfo.principal, borrowinfo.interest_index)
	}

	public fun token_info(account: &signer, tokenidx: u64): (u64, u64, u64, u64) acquires TokenInfoStore {
    	    let _ = Signer::address_of(account);
    	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
    	    let token = Vector::borrow(& tokeninfos.tokens, tokenidx);
    	    (token.total_supply, token.total_reserves, token.total_borrows, token.borrow_index)
	}
	
	public fun balance<CoinType>(account: &signer) : u64 acquires DiemToken, Tokens {
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    let sender = Signer::address_of(account);
    	    balance_of_index(libratoken.index, sender)
	}
	
	public fun balance_of_index(tokenidx: u64, account: address) : u64 acquires Tokens {
    	    let tokens = borrow_global<Tokens>(account);
    	    if(tokenidx < Vector::length(&tokens.ts)) {
    		let t = Vector::borrow(& tokens.ts, tokenidx);
    		t.value
    	    } else { 0 }
	}

	public fun borrow_balance<CoinType>(account: &signer) : u64 acquires DiemToken, Tokens, TokenInfoStore {
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    let sender = Signer::address_of(account);
	    accrue_interest(libratoken.index);
    	    borrow_balance_of_index(libratoken.index, sender)
	}
	
	public fun borrow_balance_of_index(tokenidx: u64, account: address) : u64 acquires Tokens, TokenInfoStore {
    	    // recentBorrowBalance = borrower.borrowBalance * market.borrowIndex / borrower.borrowIndex
    	    let tokens = borrow_global<Tokens>(account);
    	    let borrowinfo = Vector::borrow(& tokens.borrows, tokenidx);
	    
    	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow(& tokeninfos.tokens, tokenidx);

    	    //borrowinfo.principal * ti.borrow_index / borrowinfo.interest_index
    	    mantissa_div(mantissa_mul(borrowinfo.principal, ti.borrow_index), borrowinfo.interest_index)
	}

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
    		Vector::push_back(&mut tokens.last_exchange_rates, new_mantissa(1, 100));
    		Vector::push_back(&mut tokens.incentive_supply_indexes, new_mantissa(1, 1));
    		Vector::push_back(&mut tokens.incentive_borrow_indexes, new_mantissa(1, 1));
    		usercnt = usercnt + 1;
    	    }
	}

	fun emit_events(account: &signer, etype: u64, paras: vector<u8>, data: vector<u8>) acquires UserInfo, Tokens, TokenInfoStore {
    	    let sender = Signer::address_of(account);
    	    let info = borrow_global_mut<UserInfo>(sender);
    	    let _tokens = borrow_global<Tokens>(sender);
    	    let _banktokens = borrow_global<Tokens>(contract_address());
    	    let _tokeninfos = borrow_global<TokenInfoStore>(contract_address());
    	    Event::emit_event<ViolasEvent>(&mut info.violas_events, ViolasEvent{ etype: etype, timestamp: DiemTimestamp::now_microseconds(), paras: *&paras, data: *&data});
	}

	///////////////////////////////////////////////////////////////////////////////////

	public fun is_published(account: &signer) : bool {
    	    let sender = Signer::address_of(account);
    	    exists<Tokens>(sender)
	}

	public fun disable(account: &signer) acquires TokenInfoStore {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_supervisor(sender);
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    if(tokeninfos.disabled == false) {
    		tokeninfos.disabled = true;
    		DiemAccount::restore_withdraw_capability(Option::extract(&mut tokeninfos.withdraw_capability));
    	    }
	}
	
	public fun publish(account: &signer, userdata: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    assert(!exists<Tokens>(sender), 113);
    	    move_to(account, Tokens{ ts: Vector::empty(), borrows: Vector::empty(), last_exchange_rates: Vector::empty(), incentive_supply_indexes: Vector::empty(), incentive_borrow_indexes: Vector::empty() });
	    
    	    move_to(account, UserInfo{
    		violas_events: Event::new_event_handle<ViolasEvent>(account),
    		data: *&userdata,
    		orders: Vector::empty(),
    		order_freeslots: Vector::empty(),
    		debug: Vector::empty(),
    	    });

    	    if(sender == contract_address()) {
    		let withdraw_capability = DiemAccount::extract_withdraw_capability(account);
    		move_to(account, TokenInfoStore{
    		    supervisor: contract_address(),
    		    tokens: Vector::empty(),
    		    withdraw_capability: Option::some(withdraw_capability),
    		    disabled: false,
    		    migrated: false,
    		    version: version(),
		    incentive_rate: 0,
		    incentive_refresh_speeds_last_minute: DiemTimestamp::now_microseconds() / (60*1000*1000),
		    incentive_rate_last_minute: 0,
		});
    	    } else {
    		extend_user_tokens(sender);
    		migrate_data_impl(account);
    	    };

    	    let input = EventPublish{ userdata: userdata };
    	    emit_events(account, 0, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun register_libra_token<CoinType>(account: &signer, price_oracle: address, collateral_factor: u64, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64, tokendata: vector<u8>) : u64 acquires TokenInfoStore, Tokens, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_supervisor(sender);
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let len = Vector::length(&tokeninfos.tokens);
    	    move_to(account, DiemToken<CoinType> { coin: Diem::zero<CoinType>(), index: len });
    	    create_token(account, Diem::currency_code<CoinType>(), 0x0, price_oracle, collateral_factor, base_rate, rate_multiplier, rate_jump_multiplier, rate_kink, tokendata)
	}
	
	public fun create_token(account: &signer, currency_code: vector<u8>, owner: address, price_oracle: address, collateral_factor: u64, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64, tokendata: vector<u8>) : u64 acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_supervisor(sender);

    	    //let sender = Signer::address_of(account);

    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let len = Vector::length(&tokeninfos.tokens);
    	    let mantissa_one = new_mantissa(1, 1);

    	    Vector::push_back(&mut tokeninfos.tokens, TokenInfo {
    		currency_code: *&currency_code,
    		owner: owner,
    		total_supply: 0,
    		total_reserves: 0,
    		total_borrows: 0,
    		borrow_index: mantissa_one,
    		price: 0,
    		price_oracle: price_oracle,
    		collateral_factor: collateral_factor,
    		base_rate: base_rate/(365*24*60),
    		rate_multiplier: rate_multiplier/(365*24*60),
    		rate_jump_multiplier: rate_jump_multiplier/(365*24*60),
    		rate_kink: rate_kink,
    		last_minute: DiemTimestamp::now_microseconds() / (60*1000*1000),
		incentive_supply_index: mantissa_one,
		incentive_supply_timestamp: DiemTimestamp::now_microseconds() / (60*1000*1000),
		incentive_speed: 0,
		incentive_borrow_index: mantissa_one,
		incentive_borrow_timestamp: DiemTimestamp::now_microseconds() / (60*1000*1000),
		price_oracle_last_timestamp: 0,
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
    		base_rate: base_rate/(365*24*60),
    		rate_multiplier: rate_multiplier/(365*24*60),
    		rate_jump_multiplier: rate_jump_multiplier/(365*24*60),
    		rate_kink: rate_kink,
    		last_minute: DiemTimestamp::now_microseconds() / (60*1000*1000),
		incentive_supply_index: mantissa_one,
		incentive_supply_timestamp: DiemTimestamp::now_microseconds() / (60*1000*1000),
		incentive_speed: 0,
		incentive_borrow_index: mantissa_one,
		incentive_borrow_timestamp: DiemTimestamp::now_microseconds() / (60*1000*1000),
		price_oracle_last_timestamp: 0,
    		data: *&tokendata,
    		bulletin_first: Vector::empty(),
    		bulletins: Vector::empty()
    	    });	

    	    extend_user_tokens(contract_address());

    	    let input = EventRegisterDiemToken {
    		currency_code: currency_code,
    		price_oracle: price_oracle,
    		collateral_factor: collateral_factor,
    		base_rate: base_rate,
    		rate_multiplier: rate_multiplier,
    		rate_jump_multiplier: rate_jump_multiplier,
    		rate_kink: rate_kink,
    		tokendata: tokendata,
    	    };
	    
    	    emit_events(account, 1, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
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

    	    let input = EventMint {
    		tokenidx: tokenidx,
    		payee: payee,
    		amount: amount,
    		data: data,
    	    };
	    
    	    emit_events(account, 2, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
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
	    
    	    let input = EventTransfer {
    		tokenidx: tokenidx,
    		payee: payee,
    		amount: amount,
    		data: data,
    	    };
	    
    	    emit_events(account, 3, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

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
	    
    	    if(util <= ti.rate_kink) {
    		mantissa_mul(ti.rate_multiplier, util) + ti.base_rate
    	    } else {
    		let normalrate = mantissa_mul(ti.rate_multiplier, ti.rate_kink) + ti.base_rate;
    		let excessutil = util - ti.rate_kink;
    		mantissa_mul(ti.rate_jump_multiplier, excessutil) + normalrate
    	    }

    	    // way to calc supply_rate
    	    // let borrowrate = borrow_rate(tokenidx);
    	    // let ratetopool = mantissa_mul(borrowrate, new_mantissa(95, 100));
    	    // mantissa_mul(ratetopool, util)
	}

	fun accrue_interest(tokenidx: u64) acquires Tokens, TokenInfoStore {
    	    let borrowrate = borrow_rate(tokenidx);
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);

    	    let minute = DiemTimestamp::now_microseconds() / (60*1000*1000);
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

    		let balance = balance_of_index(i+1, account);
    		let exchange_rate = exchange_rate(i);
    		let borrow_balance = borrow_balance_of_index(i, account);
		
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
	public fun update_price_from_oracle<CoinType>(account: &signer) acquires TokenInfoStore, DiemToken, UserInfo, Tokens {
    	    let (value, timestamp) = Oracle::get_exchange_rate<CoinType>();
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());

    	    let input = EventUpdatePriceFromOracle {
    		currency_code: Diem::currency_code<CoinType>(),
    		tokenidx: libratoken.index,
    		price: FixedPoint32::get_raw_value(*&value),
    	    };
    	    emit_events(account, 16, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);

    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, libratoken.index);
    	    ti.price = FixedPoint32::get_raw_value(value);

	    if(timestamp != ti.price_oracle_last_timestamp) {
		ti.price_oracle_last_timestamp = timestamp;
	    } else {
		let delta_minutes = safe_sub(DiemTimestamp::now_microseconds(), timestamp) / (60*1000*1000);
		assert(delta_minutes < 60, 601);
	    };
	}

	public fun update_price<CoinType>(account: &signer, price: u64) acquires TokenInfoStore, UserInfo, DiemToken, Tokens {
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    update_price_index(account, Diem::currency_code<CoinType>(), libratoken.index, price);
	}
	
	public fun update_price_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, price: u64) acquires TokenInfoStore, UserInfo, Tokens {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    	    assert(ti.price_oracle == sender, 116);
    	    ti.price = price;

    	    let input = EventUpdatePrice {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		price: price,
    	    };
	    
    	    emit_events(account, 6, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun lock<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, DiemToken {
    	    update_price_from_oracle<CoinType>(account);
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    lock_index(account, Diem::currency_code<CoinType>(), libratoken.index, amount, data);
	}

	public fun lock_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_price(tokenidx);
    	    require_enabled();
	    
    	    extend_user_tokens(sender);

    	    accrue_interest(tokenidx);

	    //let incentive = lock_incentive(sender, tokenidx);
	    check_for_incentive_speeds_refresh();
	    update_incentive_supply_index(tokenidx);
	    let incentive = distribute_supply_incentive(tokenidx, sender);
	    
    	    let er = exchange_rate(tokenidx);
    	    pay_from(tokenidx, sender, contract_address(), amount);

    	    let tokens = mantissa_div(amount, er);
    	    bank_mint(tokenidx+1, sender, tokens);

    	    let input = EventLock {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		amount: amount,
    		data: data,
		incentive: incentive,
    	    };

    	    emit_events(account, 7, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun redeem<CoinType>(account: &signer, amount: u64, data: vector<u8>) : u64 acquires Tokens, TokenInfoStore, UserInfo, DiemToken {
    	    update_price_from_oracle<CoinType>(account);
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    redeem_index(account, Diem::currency_code<CoinType>(), libratoken.index, amount, data)
	}
	
	public fun redeem_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, amount: u64, data: vector<u8>) : u64 acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);

    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_price(tokenidx);
    	    require_enabled();

    	    extend_user_tokens(sender);

    	    accrue_interest(tokenidx);

	    //let incentive = lock_incentive(sender, tokenidx);
	    check_for_incentive_speeds_refresh();
	    update_incentive_supply_index(tokenidx);
	    let incentive = distribute_supply_incentive(tokenidx, sender);

    	    //assert(amount > 0, 1161);
    	    let er = exchange_rate(tokenidx);
    	    let token_amount = mantissa_div(amount, er);

	    if(amount == 0) {
		let tokens = borrow_global<Tokens>(sender);
    		let t = Vector::borrow(& tokens.ts, tokenidx+1);
		token_amount = t.value;
		amount = mantissa_mul(token_amount, er);
	    };

    	    let (sum_collateral, sum_borrow) = account_liquidity(sender, tokenidx, token_amount, 0);

    	    assert(sum_collateral+1000000 >= sum_borrow, 117);

    	    let T{ index:_, value:_ } = withdraw_from(tokenidx+1, sender, token_amount);	
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti1 = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx+1);
    	    ti1.total_supply = safe_sub(ti1.total_supply, token_amount);

    	    transfer_from(tokenidx, contract_address(), sender, amount);

    	    let input = EventRedeem {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		amount: amount,
    		data: data,
		incentive: incentive,
    	    };

    	    emit_events(account, 8, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);

	    amount
	}

	public fun borrow<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, DiemToken {
    	    update_price_from_oracle<CoinType>(account);
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    borrow_index(account, Diem::currency_code<CoinType>(), libratoken.index, amount, data);
	}

	public fun borrow_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_price(tokenidx);
    	    require_enabled();

    	    extend_user_tokens(sender);

    	    accrue_interest(tokenidx);

	    //let incentive = borrow_incentive(sender, tokenidx);
	    check_for_incentive_speeds_refresh();
	    update_incentive_borrow_index(tokenidx);
	    let incentive = distribute_borrow_incentive(tokenidx, sender);
	    
    	    let (sum_collateral, sum_borrow) = account_liquidity(sender, tokenidx, 0, amount);
    	    assert(sum_collateral >= sum_borrow, 118);

    	    let balance = borrow_balance_of_index(tokenidx, sender);

    	    let tokens = borrow_global_mut<Tokens>(sender);
    	    let borrowinfo = Vector::borrow_mut(&mut tokens.borrows, tokenidx);

    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);

    	    ti.total_borrows = ti.total_borrows + amount;
    	    borrowinfo.principal = balance + amount;
    	    borrowinfo.interest_index = ti.borrow_index;

    	    transfer_from(tokenidx, contract_address(), sender, amount);

    	    let input = EventBorrow {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		amount: amount,
    		data: data,
		incentive: incentive,
    	    };

    	    emit_events(account, 9, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	fun repay_borrow_for(sender: address, tokenidx: u64, borrower: address, amount: u64) :u64 acquires Tokens, TokenInfoStore {
    	    let balance = borrow_balance_of_index(tokenidx, borrower);
    	    assert(amount <= balance && amount > 0, 119);

    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    	    ti.total_borrows = safe_sub(ti.total_borrows, amount);

    	    let tokens = borrow_global_mut<Tokens>(borrower);
    	    let borrowinfo = Vector::borrow_mut(&mut tokens.borrows, tokenidx);
    	    borrowinfo.principal = safe_sub(balance, amount);
    	    borrowinfo.interest_index = ti.borrow_index;
	    
    	    pay_from(tokenidx, sender, contract_address(), amount);
    	    amount
	}

	public fun repay_borrow<CoinType>(account: &signer, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, DiemToken {
    	    update_price_from_oracle<CoinType>(account);
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    repay_borrow_index(account, Diem::currency_code<CoinType>(), libratoken.index, amount, data);
	}
	
	public fun repay_borrow_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_price(tokenidx);
    	    require_enabled();

    	    extend_user_tokens(sender);
	    
    	    accrue_interest(tokenidx);

	    //let incentive = borrow_incentive(sender, tokenidx);
	    check_for_incentive_speeds_refresh();
	    update_incentive_borrow_index(tokenidx);
	    let incentive = distribute_borrow_incentive(tokenidx, sender);
	    
    	    amount = repay_borrow_for(sender, tokenidx, sender, amount);

    	    let input = EventRepayBorrow {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		amount: amount,
    		data: data,
		incentive: incentive,
    	    };

    	    emit_events(account, 10, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun liquidate_borrow<CoinType1, CoinType2>(account: &signer, borrower: address, amount: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo, DiemToken {
    	    update_price_from_oracle<CoinType1>(account);
    	    update_price_from_oracle<CoinType2>(account);
    	    let libratoken1 = borrow_global<DiemToken<CoinType1>>(contract_address());
    	    let libratoken2 = borrow_global<DiemToken<CoinType2>>(contract_address());
    	    liquidate_borrow_index(account, Diem::currency_code<CoinType1>(), Diem::currency_code<CoinType2>(), libratoken1.index, borrower, amount, libratoken2.index, data);
	}
	
	public fun liquidate_borrow_index(account: &signer, currency_code1: vector<u8>, currency_code2: vector<u8>, tokenidx: u64, borrower: address, amount: u64, collateral_tokenidx: u64, data: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_first_tokenidx(collateral_tokenidx);
    	    require_price(tokenidx);
    	    require_price(collateral_tokenidx);
    	    require_enabled();

    	    extend_user_tokens(sender);
    	    extend_user_tokens(borrower);
	    
    	    accrue_interest(tokenidx);

    	    let (sum_collateral, sum_borrow) = account_liquidity(borrower, 99999, 0, 0);
    	    assert(sum_collateral < sum_borrow, 120);

    	    let borrowed = borrow_balance_of_index(tokenidx, borrower);
    	    assert(amount <= borrowed && amount > 0, 121);

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

    	    let input = EventLiquidateBorrow {
    		currency_code1: currency_code1,
    		currency_code2: currency_code2,
    		tokenidx: tokenidx,
    		borrower: borrower,
    		amount: amount,
    		collateral_tokenidx: collateral_tokenidx,
    		collateral_amount: mantissa_mul(value, exchange_rate(collateral_tokenidx)),
    		data: data,	    
    	    };

    	    emit_events(account, 11, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun update_collateral_factor<CoinType>(account: &signer, factor: u64) acquires TokenInfoStore, UserInfo, DiemToken, Tokens {
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    update_collateral_factor_index(account, Diem::currency_code<CoinType>(), libratoken.index, factor);
	}

	public fun update_collateral_factor_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, factor: u64) acquires TokenInfoStore, UserInfo, Tokens {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_supervisor(sender);
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    	    ti.collateral_factor = factor;

    	    let input = EventUpdateCollateralFactor {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		factor: factor,
    	    };
	    
    	    emit_events(account, 12, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun update_rate_model<CoinType>(account: &signer, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64) acquires TokenInfoStore, UserInfo, DiemToken, Tokens {
    	    let libratoken = borrow_global<DiemToken<CoinType>>(contract_address());
    	    update_rate_model_index(account, Diem::currency_code<CoinType>(), libratoken.index, base_rate, rate_multiplier, rate_jump_multiplier, rate_kink);
	}

	public fun update_rate_model_index(account: &signer, currency_code: vector<u8>, tokenidx: u64, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64) acquires TokenInfoStore, UserInfo, Tokens {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_first_tokenidx(tokenidx);
    	    require_supervisor(sender);
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
    	    let ti = Vector::borrow_mut(&mut tokeninfos.tokens, tokenidx);
    	    ti.base_rate = base_rate/(365*24*60);
    	    ti.rate_multiplier = rate_multiplier/(365*24*60);
    	    ti.rate_jump_multiplier = rate_jump_multiplier/(365*24*60);
    	    ti.rate_kink = rate_kink;
	    
    	    let input = EventUpdateRateModel {
    		currency_code: currency_code,
    		tokenidx: tokenidx,
    		base_rate: base_rate,
    		rate_multiplier: rate_multiplier,
    		rate_jump_multiplier: rate_jump_multiplier,
    		rate_kink: rate_kink,
    	    };
	    
    	    emit_events(account, 15, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}
	
	public fun enter_bank<CoinType>(account: &signer, amount: u64) acquires DiemToken, TokenInfoStore, Tokens, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_enabled();

	    assert(amount > 0, 701);
	    
    	    let libratoken = borrow_global_mut<DiemToken<CoinType>>(contract_address());

    	    let withdraw_capability = DiemAccount::extract_withdraw_capability(account);
    	    DiemAccount::pay_from<CoinType>(&withdraw_capability, contract_address(), amount, Vector::empty(), Vector::empty());
    	    DiemAccount::restore_withdraw_capability(withdraw_capability);
	    
    	    bank_mint(libratoken.index, sender, amount);

    	    let input = EventEnterBank {
    		currency_code: Diem::currency_code<CoinType>(),
    		tokenidx: libratoken.index,
    		amount: amount,
    	    };
	    
    	    emit_events(account, 13, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}

	public fun exit_bank<CoinType>(account: &signer, amount: u64) acquires DiemToken, TokenInfoStore, Tokens, UserInfo {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_enabled();

	    assert(amount > 0, 702);
	    
    	    let libratoken = borrow_global_mut<DiemToken<CoinType>>(contract_address());
	    
    	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
    	    DiemAccount::pay_from<CoinType>(Option::borrow(&tokeninfos.withdraw_capability), sender, amount, Vector::empty(), Vector::empty());
	    
    	    let t = withdraw_from(libratoken.index, sender, amount);
    	    bank_burn(t);

    	    let input = EventEnterBank {
    		currency_code: Diem::currency_code<CoinType>(),
    		tokenidx: libratoken.index,
    		amount: amount,
    	    };

    	    emit_events(account, 14, BCS::to_bytes(&input), Vector::empty());
    	    //debug_print(&input);
	}
	
	public fun set_incentive_rate2(account: &signer, rate: u64) acquires TokenInfoStore {
    	    let sender = Signer::address_of(account);
    	    require_published(sender);
    	    require_supervisor(sender);
	    
    	    let tokeninfos = borrow_global_mut<TokenInfoStore>(contract_address());
	    tokeninfos.incentive_rate = rate/(2*24*60);
	    tokeninfos.incentive_rate_last_minute = DiemTimestamp::now_microseconds() / (60*1000*1000);
	    
	    refresh_incentive_speeds();
	}
	
	
	// fun lock_incentive(sender: address, tokenidx: u64) : u64  acquires Tokens, TokenInfoStore {
	// 	let lock_amount = balance_of_index(tokenidx+1, sender);
	// 	let curr_exchange_rate = exchange_rate(tokenidx);
	// 	let tokens = borrow_global_mut<Tokens>(sender);
	// 	let last_exchange_rate = Vector::borrow_mut(&mut tokens.last_exchange_rates, tokenidx);

	// 	let rate_diff = mantissa_div(safe_sub(curr_exchange_rate,*last_exchange_rate), *last_exchange_rate);
	// 	let lock_interest = mantissa_mul(lock_amount, rate_diff);
	// 	let amount = mantissa_mul(lock_interest, *last_exchange_rate);
	
	// 	let price = token_price(tokenidx);
	// 	let base_amount = mantissa_mul(amount, price);

	// 	*last_exchange_rate = curr_exchange_rate;
	
	// 	if(base_amount > 0 && DiemAccount::balance<VLS>(contract_address()) > base_amount) {
	// 	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	// 	    DiemAccount::pay_from<VLS>(Option::borrow(&tokeninfos.withdraw_capability), sender, base_amount, Vector::empty(), Vector::empty());
	// 	} else { base_amount = 0; };
	// 	base_amount
	// }

	// fun borrow_incentive(sender: address, tokenidx: u64) : u64  acquires Tokens, TokenInfoStore {
	// 	let balance = borrow_balance_of_index(tokenidx, sender);

	// 	let tokens = borrow_global<Tokens>(sender);
	// 	let borrowinfo = Vector::borrow(& tokens.borrows, tokenidx);

	// 	let amount = safe_sub(balance, borrowinfo.principal);
	
	// 	let price = token_price(tokenidx);
	// 	let base_amount = mantissa_mul(amount, price);

	// 	if(base_amount > 0 && DiemAccount::balance<VLS>(contract_address()) > base_amount) {
	// 	    let tokeninfos = borrow_global<TokenInfoStore>(contract_address());
	// 	    DiemAccount::pay_from<VLS>(Option::borrow(&tokeninfos.withdraw_capability), sender, base_amount, Vector::empty(), Vector::empty());
	// 	} else { base_amount = 0; };
	// 	base_amount
	// }

	// fun debug_print<T>(_x: &T) {
	// 	Debug::print(x);
	// }

	// public fun publish2(account: &signer, a: u64, userdata: vector<u8>) acquires Tokens, TokenInfoStore, UserInfo {
	//     assert(a!=50, 7050);
	    
    	//     let sender = Signer::address_of(account);

	//     assert(a!=51, 7051);

    	//     assert(!exists<Tokens>(sender), 113);

	//     assert(a!=1, 7001);
	    
    	//     move_to(account, Tokens{ ts: Vector::empty(), borrows: Vector::empty(), last_exchange_rates: Vector::empty(), incentive_supply_indexes: Vector::empty(), incentive_borrow_indexes: Vector::empty() });
	    
	//     assert(a!=2, 7002);
	    
    	//     move_to(account, UserInfo{
    	// 	violas_events: Event::new_event_handle<ViolasEvent>(account),
    	// 	data: *&userdata,
    	// 	orders: Vector::empty(),
    	// 	order_freeslots: Vector::empty(),
    	// 	debug: Vector::empty(),
    	//     });

	//     assert(a!=3, 7003);

    	//     if(sender == contract_address()) {
    	// 	let withdraw_capability = DiemAccount::extract_withdraw_capability(account);
	// 	assert(a!=4, 7004);
    	// 	move_to(account, TokenInfoStore{
    	// 	    supervisor: contract_address(),
    	// 	    tokens: Vector::empty(),
    	// 	    withdraw_capability: Option::some(withdraw_capability),
    	// 	    disabled: false,
    	// 	    migrated: false,
    	// 	    version: version(),
	// 	    incentive_rate: 0,
	// 	    incentive_refresh_speeds_last_minute: DiemTimestamp::now_microseconds() / (60*1000*1000),
	// 	    incentive_rate_last_minute: 0,
	// 	});
	// 	assert(a!=5, 7005);
    	//     } else {
    	// 	extend_user_tokens(sender);
	// 	assert(a!=6, 7006);
    	// 	migrate_data_impl(account); 
	// 	assert(a!=7, 7007);
   	//     };

    	//     let input = EventPublish{ userdata: userdata };
    	//     emit_events(account, 0, BCS::to_bytes(&input), Vector::empty());
	//     assert(a!=8, 7008);
    	//     //debug_print(&input);
	// }
		   
    }

}

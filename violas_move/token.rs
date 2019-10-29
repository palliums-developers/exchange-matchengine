module DToken {

    import 0x00.LibraAccount;
    import 0x00.Hash;
    import 0x0.Event;
    import 0x0.U64Util;
    import 0x0.AddressUtil;
    import 0x0.BytearrayUtil;
    
    resource Owner { }

    // A resource representing the DToken
    resource T {
        value: u64,
    }

    resource Info {
        // Event handle for sent event
        sent_events: Event.Handle<Self.SentPaymentEvent>,
        // Event handle for received event
        received_events: Event.Handle<Self.ReceivedPaymentEvent>,

        publish_events: Event.Handle<Self.PublishEvent>,
        mint_events: Event.Handle<Self.MintEvent>,
        transfer_events: Event.Handle<Self.TransferEvent>,
        
        makeorder_events: Event.Handle<Self.MakeOrderEvent>,
        cancelorder_events: Event.Handle<Self.CancelOrderEvent>,
        takeorder_events: Event.Handle<Self.TakeOrderEvent>,
    }

    resource Order {
        token: Self.T,
        price: u64,
    }
    
    // Message for sent events
    struct SentPaymentEvent {
        // The address that was paid
        payee: address,
        // The amount of LibraCoin.T sent
        amount: u64,
    }

    // Message for received events
    struct ReceivedPaymentEvent {
        // The address that sent the coin
        payer: address,
        // The amount of LibraCoin.T received
        amount: u64,
    }

    struct PublishEvent {
        etype: u64, // 0
        sender: address,
        receiver: address,
        token: address,
    }
    
    struct MintEvent {
        etype: u64, // 1
        sender: address,
        receiver: address,
        token: address,
        amount: u64,
    }
    
    struct TransferEvent {
        etype: u64, // 2
        sender: address,
        receiver: address,
        token: address,
        amount: u64,
    }
    
    struct MakeOrderEvent {
        etype: u64, // 3
        sender: address,
        receiver: address,
        token: address,
        amount: u64,
        
        price: u64,
    }

    struct CancelOrderEvent {
        etype: u64, // 4
        sender: address,
        receiver: address,
        token: address,
        amount: u64,
        
        price: u64,        
    }

    struct TakeOrderEvent {
        etype: u64, // 5
        sender: address,
        receiver: address,
        token: address,
        amount: u64,
        
        price: u64,        
    }
    
    make_sure_sender_published() acquires Info {
        let sender: address;
        sender = get_txn_sender();
        if(!exists<T>(move(sender))) {
            Self.publish();
        }
        return;
    }

    // Publishes an initial zero dToken to the sender.
    public publish() acquires Info {

        let sender: address;
        sender = get_txn_sender();

        if(exists<T>(copy(sender))) {
            return;
        }

        if(!LibraAccount.exists(copy(sender))) {
            create_account(copy(sender));            
        }
        
        if (copy(sender) == 0x{dtoken}) {
            move_to_sender<Owner>(Owner{});
        }
        
        move_to_sender<T>(T{ value: 0 });

        move_to_sender<Info>(Info{
            sent_events: Event.new_event_handle<Self.SentPaymentEvent>(),
            received_events: Event.new_event_handle<Self.ReceivedPaymentEvent>(),

            publish_events: Event.new_event_handle<Self.PublishEvent>(),
            mint_events: Event.new_event_handle<Self.MintEvent>(),
            transfer_events: Event.new_event_handle<Self.TransferEvent>(),
            
            makeorder_events: Event.new_event_handle<Self.MakeOrderEvent>(),
            cancelorder_events: Event.new_event_handle<Self.CancelOrderEvent>(),
            takeorder_events: Event.new_event_handle<Self.TakeOrderEvent>(),
        });

        Self.emit_events(0, copy(sender), 0, 0);
        
        return;
    }

    require_published() {
        let sender: address;
        let is_present: bool;
        sender = get_txn_sender();
        is_present = exists<T>(move(sender));
        assert(move(is_present), 101);
        return;
    }
    
    require_owner() {
        let sender: address;
        let is_present: bool;
        sender = get_txn_sender();
        is_present = exists<Owner>(move(sender));
        assert(move(is_present), 102);
        return;
    }

    // Mint new dTokens.
    mint(value: u64): Self.T {
        Self.require_published();
        Self.require_owner();
        return T{value: move(value)};
    }

    public mint_to_address(payee: address, amount: u64) acquires T, Info {

        let token: Self.T;
        token = Self.mint(copy(amount));
        
        // Mint and deposit the coin
        Self.deposit(copy(payee), move(token));

        Self.emit_events(1, copy(payee), copy(amount), 0);
        
        return;
    }
    
    value(coin_ref: &Self.T): u64 {
        return *&move(coin_ref).value;
    }
    
    // Returns an account's dToken balance.
    balance(): u64  acquires T {
        let sender: address;
        let token_ref: &Self.T;
        let token_value: u64;

        Self.require_published();
        
        sender = get_txn_sender();
        token_ref = borrow_global<T>(move(sender));
        token_value = *(&move(token_ref).value);

        return move(token_value);
    }

    // Deposit owned tokens to a payee's address
    deposit(payee: address, to_deposit: Self.T) acquires T {
        let sender: address;
        let amount: u64;
        
        let payee_token_ref: &mut Self.T;
        let payee_token_value: u64;
        let to_deposit_value: u64;

        let sender_info_ref: &mut Self.Info;
        let payee_info_ref: &mut Self.Info;
        let sent_event: Self.SentPaymentEvent;
        let received_event: Self.ReceivedPaymentEvent;
        
        
        Self.require_published();

        sender = get_txn_sender();
        
        payee_token_ref = borrow_global_mut<T>(copy(payee));
        payee_token_value = *(&copy(payee_token_ref).value);

        // Unpack and destroy to_deposit tokens
        T{ value: to_deposit_value } = move(to_deposit);

        amount = copy(to_deposit_value);
        
        // Increase the payees balance with the destroyed token amount
        *(&mut move(payee_token_ref).value) = move(payee_token_value) + move(to_deposit_value);

        return;
    }
    
    // Withdraw an amount of tokens of the sender and return it.
    withdraw(amount: u64): Self.T acquires T {
        let sender: address;
        let sender_token_ref: &mut Self.T;
        let value: u64;

        Self.require_published();

        sender = get_txn_sender();
        
        sender_token_ref = borrow_global_mut<T>(move(sender));
        value = *(&copy(sender_token_ref).value);

        // Make sure that sender has enough tokens
        assert(copy(value) >= copy(amount), 103);

        // Split the senders token and return the amount specified
        *(&mut move(sender_token_ref).value) = move(value) - copy(amount);
        return T{ value: move(amount) };
    }

    public pay_from_sender(payee: address, amount: u64) acquires T, Info {
        let sender: address;
        let to_pay: Self.T;

        Self.require_published();

        sender = get_txn_sender();
        
        to_pay = Self.withdraw(copy(amount));
        Self.deposit(copy(payee), move(to_pay));

        Self.emit_events(2, copy(payee), copy(amount), 0);
        
        return;
    }


    public make_order(amount: u64, price: u64) acquires T, Info {
        let sender: address;
        let token: Self.T;
        let sender_info_ref: &mut Self.Info;
        let makeorder_event: Self.MakeOrderEvent;

        Self.require_published();

        sender = get_txn_sender();
        
        token = Self.withdraw(copy(amount));
        move_to_sender<Order>(Order { token: move(token), price: copy(price)});

        Self.emit_events(3, copy(sender), copy(amount), copy(price));
        
        return;
    }

    public cancel_order() acquires T, Order, Info {
        let sender: address;
        let order: Self.Order;
        let token: Self.T;
        let price: u64;
        let sender_info_ref: &mut Self.Info;
        let cancelorder_event: Self.CancelOrderEvent;
        let amount: u64;
        
        Self.require_published();
        
        sender = get_txn_sender();
        
        order = move_from<Order>(copy(sender));

        Order { token:token, price:price } =  move(order);

        amount = Self.value(&token);

        Self.deposit(copy(sender), move(token));

        Self.emit_events(4, copy(sender), copy(amount), copy(price));
        
        return;
    }
    
    // Currently only support buy whole order, not support split order.
    public take_order(maker: address) acquires T, Order, Info {
        let sender: address;
        let order: Self.Order;
        let token: Self.T;
        let price: u64;

        let sender_info_ref: &mut Self.Info;
        let takeorder_event: Self.TakeOrderEvent;
        let amount: u64;
        
        Self.require_published();
        
        sender = get_txn_sender();

        order = move_from<Order>(copy(maker));
        Order { token:token, price:price } = move(order);

        amount = Self.value(&token);
        
        LibraAccount.pay_from_sender(copy(maker), copy(price));

        Self.deposit(copy(sender), move(token));

        Self.emit_events(5, copy(maker), copy(amount), copy(price));
        
        return;
    }

    emit_events(etype: u64, receiver: address, amount: u64, price: u64) acquires Info {

        let sender: address;
        let token: address;
        
        let publish_event: Self.PublishEvent;
        let mint_event: Self.MintEvent;
        let transfer_event: Self.TransferEvent;
        
        let makeorder_event: Self.MakeOrderEvent;
        let cancelorder_event: Self.CancelOrderEvent;
        let takeorder_event: Self.TakeOrderEvent;

        let sender_info_ref: &mut Self.Info;
        let receiver_info_ref: &mut Self.Info;
        
        sender = get_txn_sender();
        token = 0x{dtoken};

        // publish
        if(copy(etype) == 0) {
            
            publish_event = PublishEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
            };
            sender_info_ref = borrow_global_mut<Info>(copy(sender));
            Event.emit_event<Self.PublishEvent>(&mut move(sender_info_ref).publish_events, move(publish_event));
            return;
        }

        // mint
        if(copy(etype) == 1) {
            
            mint_event = MintEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
                amount: move(amount),
            };
            if(true) {
                sender_info_ref = borrow_global_mut<Info>(copy(sender));
                Event.emit_event<Self.MintEvent>(&mut move(sender_info_ref).mint_events, copy(mint_event));
            }
            if(true) {
                receiver_info_ref = borrow_global_mut<Info>(copy(receiver));  
                Event.emit_event<Self.MintEvent>(&mut move(receiver_info_ref).mint_events, move(mint_event));
            }            
            return;
        }

        // transfer
        if(copy(etype) == 2) {
            
            transfer_event = TransferEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
                amount: move(amount),
            };
            if(true) {
                sender_info_ref = borrow_global_mut<Info>(copy(sender));
                Event.emit_event<Self.TransferEvent>(&mut move(sender_info_ref).transfer_events, copy(transfer_event));
            }
            if(true) {
                receiver_info_ref = borrow_global_mut<Info>(copy(receiver));  
                Event.emit_event<Self.TransferEvent>(&mut move(receiver_info_ref).transfer_events, move(transfer_event));
            }            
            return;
        }
        
        // make order
        if(copy(etype) == 3) {
            
            makeorder_event = MakeOrderEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
                amount: move(amount),
                price: move(price)
            };
            sender_info_ref = borrow_global_mut<Info>(copy(sender));
            Event.emit_event<Self.MakeOrderEvent>(&mut move(sender_info_ref).makeorder_events, move(makeorder_event));
            return;
        }

        // cancel order
        if(copy(etype) == 4) {
            
            cancelorder_event = CancelOrderEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
                amount: move(amount),
                price: move(price)
            };
            sender_info_ref = borrow_global_mut<Info>(copy(sender));
            Event.emit_event<Self.CancelOrderEvent>(&mut move(sender_info_ref).cancelorder_events, move(cancelorder_event));
            return;
        }

        // make order
        if(copy(etype) == 5) {
            
            takeorder_event = TakeOrderEvent {
                etype: copy(etype),
                sender: copy(sender),
                receiver: copy(receiver),
                token: copy(token),
                amount: move(amount),
                price: move(price)
            };
            if(true) {
                sender_info_ref = borrow_global_mut<Info>(copy(sender));
                Event.emit_event<Self.TakeOrderEvent>(&mut move(sender_info_ref).takeorder_events, copy(takeorder_event));
            }
            if(true) {
                receiver_info_ref = borrow_global_mut<Info>(copy(receiver));  
                Event.emit_event<Self.TakeOrderEvent>(&mut move(receiver_info_ref).takeorder_events, move(takeorder_event));
            }
            return;
        }
        
        return;
    }
}










// let seq: u64;
// let sender_info_ref2: &mut Self.Info;
// move_to_sender<Info>(Info{ sequence_number: 0, sent_events: Event.new_event_handle<Self.SentPaymentEvent>(), received_events: Event.new_event_handle<Self.ReceivedPaymentEvent>()});
// if (true) {
//     sender_info_ref2 = borrow_global_mut<Info>(copy(sender));
//     seq = *(&mut copy(sender_info_ref2).sequence_number);
//     *(&mut move(sender_info_ref2).sequence_number) = move(seq) + 1;
// }

// let aaa: &Self.T;
// aaa = &token;
// amount = *(&move(aaa).value);

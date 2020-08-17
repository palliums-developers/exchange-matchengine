address 0x1 {

module Oracle {

use 0x1::Libra::{Self};
use 0x1::LibraTimestamp;
use 0x1::Signer;
use 0x1::CoreAddresses;
use 0x1::FixedPoint32::{Self, FixedPoint32};
use 0x1::Event::{ Self, EventHandle };

    /// Evnet for updating
    struct UpdateEvent {
        value : FixedPoint32,
        timestamp : u64,
        currency_code : vector<u8>        
    }

    /// Exchange rate for a currency / USD
    resource struct ExchangeRate<CoinType> { 
        value : FixedPoint32,
        timestamp : u64,    //Unix time in microseconds
        /// Event stream for updating exchange rate and where `update_exchange_rate`s will be emitted.
        update_events: EventHandle<UpdateEvent>
    }

    const EINVALID_SINGLETON_ADDRESS: u64 = 0;
    const ENOT_A_REGISTERED_CURRENCY: u64 = 1;

    fun emit_updating_events<CoinType>( exchange_rate: &mut ExchangeRate<CoinType>)
    {
        Event::emit_event(
                &mut exchange_rate.update_events,                
                UpdateEvent {                    
                    value : *& exchange_rate.value,
                    timestamp : exchange_rate.timestamp,
                    currency_code : Libra::currency_code<CoinType>()                    
                }
            );
    }

    /// update exchange rate, if the curreny pair doesn't exist the create it 
    public fun update_exchange_rate<CoinType>(
        lr_account : &signer, 
        numerator : u64, 
        denominator: u64
    ) acquires ExchangeRate {
        assert(
            Signer::address_of(lr_account) == CoreAddresses::LIBRA_ROOT_ADDRESS(),
            EINVALID_SINGLETON_ADDRESS
        );        

        assert(Libra::is_currency<CoinType>(), ENOT_A_REGISTERED_CURRENCY);        

        if(!exists<ExchangeRate<CoinType>>(CoreAddresses::LIBRA_ROOT_ADDRESS())) {
            let exchange_rate = ExchangeRate<CoinType> {
                value : FixedPoint32::create_from_rational(numerator, denominator), 
                timestamp : LibraTimestamp::now_microseconds(),
                update_events : Event::new_event_handle<UpdateEvent>(lr_account)
            };
        
            emit_updating_events(&mut exchange_rate);

            move_to(
                lr_account, 
                exchange_rate,
                );
        }
        else {
            let exchange_rate = borrow_global_mut<ExchangeRate<CoinType>>(CoreAddresses::LIBRA_ROOT_ADDRESS());

            exchange_rate.value = FixedPoint32::create_from_rational(numerator, denominator);
            exchange_rate.timestamp = LibraTimestamp::now_microseconds();

            emit_updating_events(exchange_rate);
        }
        
    }

    public fun get_exchange_rate<CoinType>() : (FixedPoint32, u64) acquires ExchangeRate    
    {
        assert(Libra::is_currency<CoinType>(), ENOT_A_REGISTERED_CURRENCY);        

        let exchange_rate = borrow_global<ExchangeRate<CoinType>>(CoreAddresses::LIBRA_ROOT_ADDRESS());

        (*&exchange_rate.value, exchange_rate.timestamp)
    }
}


}